module Collector
  class Handler
    class VblobNode < ServiceNodeHandler
      def service_type
        "vblob"
      end

      register Components::VBLOB_NODE
    end
  end
end
