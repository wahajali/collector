require 'collector/kairos_connection'

module Collector
  class Historian
    class Kairos
      attr_reader :connection
      def initialize(host, port, metric_name, protocol)
        @host = host
        @port = port
        @metric_name = metric_name
        @protocol = protocol
        @connection = EventMachine.connect(@host, @port, KairosConnection)
      end

      def send_data(properties)
        tags = (properties[:tags].flat_map do |key, value|
          Array(value).map do |v|
            "#{key}=#{v}"
          end
        end).sort.join(" ")

        binding.pry
        if @protocol == "telnet"
          command = "put #{properties[:key]} #{properties[:timestamp]} #{properties[:value]} #{tags}\n"
        elsif @protocol == "rest"
          #other option is to set it via em-http...ie create a new class and extend from httpconnection ...
          command = "POST /api/v1/datapoints HTTP/1.1\r\nContent-Type: application/json\r\nConnection: Keep-Alive\r\nHost: localhost:8080\r\nContent-Length: 82\r\n\r\n{\"name\":\"test124\",\"timestamp\":\"1394313125660\",\"value\":1234,\"tags\":{\"host\":\"test\"}}"
        end
        @connection.send_data(command)
      end
    end
  end
end
