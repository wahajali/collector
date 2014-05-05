module Collector
  class Historian
    class Kairos
    include HTTParty

      persistent_connection_adapter({ :idle_timeout => 30, :keep_alive => 30 })

      def initialize(api_host, http_client)
        @api_host = api_host
        @http_client = http_client
      end

      def send_data(data)
        send_metrics(formatted_metric_for_data(data))
      end

      private

      def formatted_metric_for_data(data)
        data[:name] = data[:tags][:name].gsub '.', '_'
        data[:tags][:name] = data[:key]
        data.delete :key
=begin
        File.open('logs/data.logs', 'a') do |file| 
          file.write(data.to_s + "\n") 
        end
        File.open('logs/name.logs', 'a') do |file| 
          file.write(data[:tags][:name] + "\n")
        end
=end
        data
      end

      def send_metrics(data)
        #TODO remove this condition in production
        return unless data[:name] == "kairosdb-benchmark" || data[:name] == "DEA"
        Config.logger.debug("Sending metrics to kairos: [#{data.inspect}]")
        body = Yajl::Encoder.encode(data)
        response = @http_client.post(@api_host, body: body, headers: {"Content-type" => "application/json"})
        if response.success?
          Config.logger.info("collector.emit-kairos.success", number_of_metrics: 1, lag_in_seconds: 0)
        else
=begin
          File.open('logs/failed.logs', 'a') do |file| 
            file.write(response.to_s + "\n")
            file.write(data.to_s + "\n") 
          end
=end
          Config.logger.warn("collector.emit-kairos.fail", number_of_metrics: 1, lag_in_seconds: 0)
        end
      end
    end
  end
end
