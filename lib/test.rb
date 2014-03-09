$:.unshift(File.expand_path(".", File.dirname(__FILE__)))
require "em-http-request"
require "eventmachine"

require "collector/config"
require "collector/kairos_connection"
require "collector/historian"
require "pry"

class Test
  def initialize
    config_file = ENV["CONFIG_FILE"] || File.expand_path("../config/config.yml", File.dirname(__FILE__))
    Collector::Config.configure(YAML.load_file(config_file))
    @historian = ::Collector::Historian.build
    EM.add_periodic_timer(4) { fetch_healthz }
  end

  def fetch_healthz
    index = 2
    host = 'host'
    is_healthy = 1
    job = 'test job'
    send_healthz_metric(is_healthy, job, index, host)
  end

  def send_healthz_metric(is_healthy, job, index, host)
    ::Collector::Config.logger.info("collector.healthz-metrics.sending", job: job, index: index)
    @historian.send_data({
      key: "healthy",
      timestamp: Time.now.to_i,
      value: is_healthy,
      tags: {job: 'a', job2: 'b'}
    })
  end
end

EM.run do
  Test.new
end
