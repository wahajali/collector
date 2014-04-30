module Collector
  class Handler
    class Dea < Handler
      def additional_tags(context)
        #NOTE remove stack since this is an array (causes problems with kairosDB)
        { #stack: context.varz["stacks"],
          ip: context.varz["host"].split(":").first,
        }
      end

      def process(context)
        send_metric("can_stage", context.varz["can_stage"], context)
        send_metric("reservable_stagers", context.varz["reservable_stagers"], context)
        send_metric("available_memory_ratio", context.varz["available_memory_ratio"], context)
        send_metric("available_disk_ratio", context.varz["available_disk_ratio"], context)

        state_metrics, application_metrics = state_counts(context)
        state_metrics.each do |state, count|
          send_metric("dea_registry_#{state.downcase}", count, context)
        end

        application_metrics[:instances].each do |key, val|
          ["used_memory_in_bytes", "state_born_timestamp", "state_running_timestamp", "used_disk_in_bytes", "computed_pcpu"].each do |m|
            #tags = { name: "#{val["application_name"]}/#{val["instance_index"]}", job: 'Application', index: val["instance_index"] }
            tags = { name: "#{val["application_name"]}", job: 'Application', index: val["instance_index"] }
            send_app_metric(m, val[m], context, tags)
          end
        end

        metrics = registry_usage(context)
        send_metric("dea_registry_mem_reserved", metrics[:mem], context)
        send_metric("dea_registry_disk_reserved", metrics[:disk], context)

        send_metric("total_warden_response_time_in_ms", context.varz["total_warden_response_time_in_ms"], context)
        send_metric("warden_request_count", context.varz["warden_request_count"], context)
        send_metric("warden_error_response_count", context.varz["warden_error_response_count"], context)

        send_metric("warden_error_response_count", context.varz["warden_error_response_count"], context)
      end

      #Rewrote method from handler to change somethings specific to application data
      # Sends the metric to the metric collector (historian)
      #
      # @param [String] name the metric name
      # @param [String, Fixnum] value the metric value
      #NOTE by default the metric name is set to the measuring value for kairos I change that inside the kairos class and swap it with the name tag
      def send_app_metric(name, value, context, tags = {})
        tags.merge!(deployment: Config.deployment_name)
        tags.merge!(dea: "#{@job}/#{context.index}", dea_index: context.index)

        @historian.send_data({
          key: name,
          timestamp: context.now,
          value: value,
          tags: tags
        })
      end

      private

      DEA_STATES = %W[
        BORN STARTING RUNNING STOPPING STOPPED CRASHED RESUMING DELETED
      ].freeze

      APPLICATION_METRICS = %W[ 
        instance_index application_name used_memory_in_bytes used_disk_in_bytes computed_pcpu
      ].freeze
      APPLICATION_METRICS_TIMESTAMPS = %W[state_running_timestamp state_born_timestamp].freeze
      def state_counts(context)
        metrics = DEA_STATES.each.with_object({}) { |s, h| h[s] = 0 }
        applications = {} 
        applications[:instances] = {} 

        context.varz["instance_registry"].each do |_, instances|
          instances.each do |k, instance|
            metrics[instance["state"]] += 1
            #TODO application host will always be the host ip of the DEA..?
            #TODO each DEA can have more than one instance of each application (application name + index are unique)
            app_data = APPLICATION_METRICS.each.with_object({}) do |key, h|
              h[key] = instance[key]
            end
            app_data["computed_pcpu"] = app_data["computed_pcpu"] * 100 unless app_data["computed_pcpu"].nil?

            tmp = APPLICATION_METRICS_TIMESTAMPS.each.with_object({}) do |key, h|
              next  if instance[key].nil?
              h[key] = (instance[key] * 1000).truncate
            end
            app_data.merge! tmp
            applications[:instances].update({ k => app_data })
          end
        end
        Config.logger.info("collector.handler.dea.", { "application_count" => applications[:instances].size })
        [metrics, applications]
      end

      RESERVING_STATES = %W[BORN STARTING RUNNING RESUMING].freeze

      def registry_usage(context)
        reserved_mem = reserved_disk = 0

        context.varz["instance_registry"].each do |_, instances|
          instances.each do |_, instance|
            if RESERVING_STATES.include?(instance["state"])
              reserved_mem += instance["limits"]["mem"]
              reserved_disk += instance["limits"]["disk"]
            end
          end
        end

        {mem: reserved_mem, disk: reserved_disk}
      end

      register Components::DEA_COMPONENT
    end
  end
end
