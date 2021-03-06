module Collector
  class Historian
    class Kairos
    include HTTParty

      persistent_connection_adapter({ :idle_timeout => 30, :keep_alive => 30 })

      def initialize(api_host, http_client, deployment_name)
        @api_host = api_host
        @http_client = http_client
        @deployment_name = deployment_name
      end

      def send_data(data)
        send_metrics(formatted_metric_for_data(data))
      end

      private

      def formatted_metric_for_data(data)
        data[:name] = "#{@deployment_name}/#{data[:tags][:name].gsub('.', '_')}"
        data[:tags][:name] = data[:key]
        data.delete :key
        data
      end

      def send_metrics(data)
        return if data[:value] == "default"
        Config.logger.debug("Sending metrics to kairos: [#{data.inspect}]")
        body = Yajl::Encoder.encode(data)
        response = @http_client.post(@api_host, body: body, headers: {"Content-type" => "application/json"})
        if response.success?
          Config.logger.info("collector.emit-kairos.success", number_of_metrics: 1, lag_in_seconds: 0)
        else
          Config.logger.warn("collector.emit-kairos.fail", number_of_metrics: 1, lag_in_seconds: 0)
        end
      end
    end
  end
end
