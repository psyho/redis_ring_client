module RedisRing
  module Client

    class UnknownShardError < StandardError; end

    class RingMetaData

      attr_reader :host, :port

      def initialize(host, port)
        @host = host
        @port = port
        @loaded = false
      end

      def reload!
        json = get_shards_json_string
        hash = JSON.parse(json)

        @ring_size = hash['count']
        @shards = (0...@ring_size).map{|n| ShardMetaData.from_json(hash['shards'][n.to_s])}

        @loaded = true
      end

      def ring_size
        reload! if should_reload?

        return @ring_size
      end

      def shard(shard_number)
        reload! if should_reload?

        unless shard_number >= 0 && shard_number < ring_size
          raise UnknownShardError.new("Shard number invalid: #{shard_number}. Ring size: #{ring_size}")
        end

        return @shards[shard_number]
      end

      protected

      def should_reload?
        !@loaded
      end

      def get_shards_json_string
        Net::HTTP.get(host, '/shards', port)
      end

    end

    class ShardMetaData

      attr_reader :host, :port, :status

      def initialize(host, port, status)
        @host = host
        @port = port
        @status = status
      end

      def self.from_json(hash)
        new(hash['host'], hash['port'].to_i, hash['status'].to_sym)
      end

    end

  end
end

