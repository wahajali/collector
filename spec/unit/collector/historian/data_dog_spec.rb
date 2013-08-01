require File.expand_path("../../../spec_helper", File.dirname(__FILE__))

class FakeResponse
  attr_accessor :success

  def initialize(success)
    @success = success
  end

  def success?
    @success
  end
end

class FakeHttpClient
  attr_reader :last_post
  attr_accessor :respond_successfully

  def initialize
    @respond_successfully = true
  end

  def post(path, options)
    @last_post = {
      path: path,
      options: options
    }

    FakeResponse.new(@respond_successfully)
  end

  def reset
    @last_post = nil
  end

  def parsed_post_body
    Yajl::Parser.parse(@last_post[:options][:body])
  end
end

describe Collector::Historian::DataDog do
  describe "sending data to DataDog" do
    let(:fake_http_client) {FakeHttpClient.new}
    let(:datadog_historian) do
      Timecop.freeze(Time.at(time)) do
        described_class.new("API_KEY", fake_http_client)
      end
    end
    let(:time) { Time.now.to_i }
    let(:datadog_metric_payload) do
      {
        key: "some_metric.some_key",
        timestamp: time,
        value: 2,
        tags: {
          job: "Test",
          index: 1,
          component: "unknown",
          service_type: "unknown",
          tag: "value",
          foo: %w(bar baz)
        }
      }
    end
    let(:expected_tags) { %w[job:Test index:1 component:unknown service_type:unknown tag:value foo:bar foo:baz] }

    before do
      ::Collector::Config.stub(:deployment_name).and_return("dev114cw")

      @counter = 0
    end

    def submit_n_events(n)
      n.times do
        datadog_historian.send_data(datadog_metric_payload.merge(value: @counter))
        @counter += 1
      end
    end

    it "aggregates the passed in data and sends the post to data dog once it hits 100 data points" do
      ::Collector::Config.logger.should_not_receive(:warn)
      ::Collector::Config.logger.should_receive(:info).with("collector.emit-datadog.success", number_of_metrics: 100).twice

      Timecop.freeze(Time.at(time + 1)) do
        submit_n_events(99)
        fake_http_client.last_post.should be_nil

        submit_n_events(1)
        fake_http_client.last_post[:path].should == "https://app.datadoghq.com/api/v1/series"
        fake_http_client.last_post[:options][:query].should == {api_key: "API_KEY"}
        fake_http_client.last_post[:options][:headers].should == {"Content-type" => "application/json"}
        body = fake_http_client.parsed_post_body
        body["series"].length.should equal(100)
        values = body["series"].map {|entry| entry["points"][0][1]}
        values.should eql((0...100).to_a)

        fake_http_client.reset
        submit_n_events(99)
        fake_http_client.last_post.should be_nil

        submit_n_events(1)
        body = fake_http_client.parsed_post_body
        body["series"].length.should equal(100)
        values = body["series"].map {|entry| entry["points"][0][1]}
        values.should eql((100...200).to_a)
      end
    end

    it "aggregates the passed in data and sends the post to data dog after 10 seconds" do
      ::Collector::Config.logger.should_not_receive(:warn)
      ::Collector::Config.logger.should_receive(:info).with("collector.emit-datadog.success", number_of_metrics: 7)

      Timecop.freeze(Time.at(time)) do
        submit_n_events(1)
      end
      fake_http_client.last_post.should be_nil

      Timecop.freeze(Time.at(time + 9)) do
        submit_n_events(5)
      end
      fake_http_client.last_post.should be_nil

      Timecop.freeze(Time.at(time + 10)) do
        submit_n_events(1)
      end

      fake_http_client.last_post[:path].should == "https://app.datadoghq.com/api/v1/series"
      fake_http_client.last_post[:options][:query].should == {api_key: "API_KEY"}
      fake_http_client.last_post[:options][:headers].should == {"Content-type" => "application/json"}
      body = fake_http_client.parsed_post_body
      body["series"].length.should equal(7)
      values = body["series"].map {|entry| entry["points"][0][1]}
      values.should eql((0...7).to_a)
    end

    it "batches more than once" do
      Timecop.freeze(Time.at(time + 11)) do
        submit_n_events(1)
      end
      fake_http_client.last_post.should_not be_nil

      fake_http_client.reset

      Timecop.freeze(Time.at(time + 15)) do
        submit_n_events(1)
      end
      fake_http_client.last_post.should be_nil

      Timecop.freeze(Time.at(time + 26)) do
        submit_n_events(1)
      end
      fake_http_client.last_post.should_not be_nil
    end

    it "converts the properties hash into a DataDog point" do
      ::Collector::Config.logger.should_not_receive(:warn)
      ::Collector::Config.logger.should_receive(:info).with("collector.emit-datadog.success", number_of_metrics: 1)

      Timecop.freeze(Time.at(time + 10)) do
        datadog_historian.send_data(datadog_metric_payload)
      end

      expected_json = Yajl::Encoder.encode({
                                             series: [
                                               {
                                                 metric: "cf.collector.some_metric.some_key",
                                                 points: [[time, 2]],
                                                 type: "gauge",
                                                 tags: expected_tags
                                               }
                                             ]
                                           })

      fake_http_client.last_post[:path].should == "https://app.datadoghq.com/api/v1/series"
      fake_http_client.last_post[:options][:query].should == {api_key: "API_KEY"}
      fake_http_client.last_post[:options][:headers].should == {"Content-type" => "application/json"}
      fake_http_client.last_post[:options][:body].should == expected_json
    end

    context "when the passed in data is missing a timestamp" do
      it "uses now" do
        datadog_metric_payload.delete(:timestamp)
        ::Collector::Config.logger.should_not_receive(:warn)
        ::Collector::Config.logger.should_receive(:info).with("collector.emit-datadog.success", number_of_metrics: 1)

        Timecop.freeze(Time.at(time + 10)) do
          datadog_historian.send_data(datadog_metric_payload)
        end

        fake_http_client.parsed_post_body["series"][0]["points"][0][0].should == time + 10
      end
    end

    context "when the api request fails" do
      it "logs" do
        fake_http_client.respond_successfully = false
        ::Collector::Config.logger.should_not_receive(:info)
        ::Collector::Config.logger.should_receive(:warn).with("collector.emit-datadog.fail", number_of_metrics: 1)

        Timecop.freeze(Time.at(time + 10)) do
          datadog_historian.send_data(datadog_metric_payload)
        end
      end
    end
  end
end