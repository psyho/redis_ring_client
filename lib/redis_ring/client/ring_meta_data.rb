module RedisRing
  module Client

    class UnknownShardError < StandardError; end

    class RingMetaData

      attr_reader :zookeeper_addr, :zookeeper, :cluster_name

      def initialize(zookeeper_addr, cluster_name)
        @zookeeper_addr = zookeeper_addr
        @cluster_name = cluster_name
      end

      def reload!
        json = get_shards_json_string
        hash = JSON.parse(json)

        @ring_size = hash['ring_size']
        @shards = (0...@ring_size).map{|n| ShardMetaData.from_json(hash['shards'][n.to_s])}
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
        !@zookeeper || @watcher.completed?
      end

      def get_shards_json_string(retries = 0)
        @zookeeper ||= self.class.zookeeper(zookeeper_addr)
        @watcher = Zookeeper::WatcherCallback.new
        resp = @zookeeper.get(:path => "/#{cluster_name}_cluster_status", :watcher => @watcher, :watcher_context => "/#{cluster_name}/cluster_status")
        return resp[:data]
      rescue ZookeeperExceptions::ZookeeperException::ConnectionClosed
        raise if retries == 4
        @zookeeper.reopen
        return get_shards_json_string(retries + 1)
      end

      def self.zookeepers
        @zookeepers ||= {}
      end

      # it appears that only a very limited number of zookeeper connections can be opened by a single client
      # so we have to cache/share them between RingMetaData instances
      def self.zookeeper(address)
        self.zookeepers[address] ||= Zookeeper.new(address)
      end

    end

    class ShardMetaData

      attr_reader :host, :port, :status

      def initialize(host, port, status)
        @host = host
        @port = port
        @status = status
        @sym = :"Redis<#{host}:#{port}>"
      end

      def self.from_json(hash)
        new(hash['host'], hash['port'].to_i, hash['status'].to_sym)
      end

      def to_sym
        @sym
      end

    end

  end
end

