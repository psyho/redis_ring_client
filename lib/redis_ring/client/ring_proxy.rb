module RedisRing
  module Client

    class RingProxy

      include OperationDefinitions

      single_key_operation :[]
      single_key_operation :[]=
      single_key_operation :append
      single_key_operation :decr
      single_key_operation :decrby
      single_key_operation :exists
      single_key_operation :expire
      single_key_operation :expireat
      single_key_operation :get
      single_key_operation :getset
      single_key_operation :hdel
      single_key_operation :hexists
      single_key_operation :hget
      single_key_operation :hgetall
      single_key_operation :hincrby
      single_key_operation :hkeys
      single_key_operation :hlen
      single_key_operation :hmget
      single_key_operation :hmset
      single_key_operation :hset
      single_key_operation :hsetnx
      single_key_operation :hvals
      single_key_operation :incr
      single_key_operation :incrby
      single_key_operation :lindex
      single_key_operation :linsert
      single_key_operation :llen
      single_key_operation :lpop
      single_key_operation :lpush
      single_key_operation :lpushx
      single_key_operation :lrange
      single_key_operation :lrem
      single_key_operation :lset
      single_key_operation :ltrim
      single_key_operation :mapped_hmget
      single_key_operation :mapped_hmset
      single_key_operation :move
      single_key_operation :persist
      single_key_operation :publish
      single_key_operation :rpop
      single_key_operation :rpush
      single_key_operation :rpushx
      single_key_operation :sadd
      single_key_operation :scard
      single_key_operation :set
      single_key_operation :setex
      single_key_operation :setnx
      single_key_operation :sismember
      single_key_operation :smembers
      single_key_operation :sort
      single_key_operation :spop
      single_key_operation :srandmember
      single_key_operation :srem
      single_key_operation :strlen
      single_key_operation :substr
      single_key_operation :ttl
      single_key_operation :type
      single_key_operation :zadd
      single_key_operation :zcard
      single_key_operation :zcount
      single_key_operation :zincrby
      single_key_operation :zrange
      single_key_operation :zrangebyscore
      single_key_operation :zrank
      single_key_operation :zrem
      single_key_operation :zremrangebyrank
      single_key_operation :zremrangebyscore
      single_key_operation :zrevrange
      single_key_operation :zrevrank
      single_key_operation :zscore

      scather_gather_operation :auth, :last_result
      scather_gather_operation :bgrewriteaof, :last_result
      scather_gather_operation :bgsave, :last_result
      scather_gather_operation :config, :last_result
      scather_gather_operation :dbsize, :sum
      scather_gather_operation :flushall, :last_result
      scather_gather_operation :flushdb, :last_result
      scather_gather_operation :keys, :sum
      scather_gather_operation :quit, :last_result
      scather_gather_operation :save, :last_result
      scather_gather_operation :select, :last_result
      scather_gather_operation :shutdown, :last_result

      # it might be useful to combine those, but it would break the interface
      unsupported_operation :info

      # could be used in a single server, but it's complicated
      # maybe a TODO for the future
      unsupported_operation :multi
      unsupported_operation :watch
      unsupported_operation :unwatch
      unsupported_operation :discard
      unsupported_operation :exec
      unsupported_operation :pipelined

      # there's no good way to scather_gather this
      unsupported_operation :monitor

      # maybe max or min from the shards?
      unsupported_operation :lastsave

      unsupported_operation :debug

      # no way to determine which shards they fall into
      unsupported_operation :psubscribe
      unsupported_operation :punsubscribe
      unsupported_operation :subscribed?

      unsupported_operation :sync
      unsupported_operation :slaveof

      random_shard_operation :echo
      random_shard_operation :ping
      random_shard_operation :randomkey

      multi_key_operation :blpop
      multi_key_operation :brpop
      multi_key_operation :del
      multi_key_operation :mapped_mget
      multi_key_operation :mget
      multi_key_operation :rename
      multi_key_operation :renamenx
      multi_key_operation :rpoplpush
      multi_key_operation :sdiff
      multi_key_operation :sdiffstore
      multi_key_operation :sinter
      multi_key_operation :sinterstore
      multi_key_operation :subscribe
      multi_key_operation :sunion
      multi_key_operation :sunionstore
      multi_key_operation :unsubscribe

      mapped_set_operation :mapped_mset
      mapped_set_operation :mapped_msetnx
      regular_set_operation :mset
      regular_set_operation :msetnx

      multi_zstore_operation :zinterstore
      multi_zstore_operation :zunionstore

      def smove(source, destination, member)
        return single_connection_operation(:smove, [source, destination], source, destination, member)
      end

      def initialize(opts = {})
        @zookeeper = opts[:zookeeper] || 'localhost:2181'
        @db = opts[:db] || 0
        @password = opts[:password]
      end

      def connection_for_key(key)
        shard = sharder.shard_for_key(key)
        return connection_pool.connection(shard)
      end

      def each_connection(&block)
        threads = []
        ring_meta_data.ring_size.times do |shard_no|
          threads << Thread.new do
            block.call(connection_pool.connection(shard_no))
          end
        end
        threads.each { |thread| thread.join }
      end

      protected

      def ring_meta_data
        @ring_meta_data ||= RingMetaData.new(@zookeeper)
      end

      def sharder
        @sharder ||= Sharder.new(ring_meta_data)
      end

      def connection_pool
        @connection_pool ||= ShardConnectionPool.new(ring_meta_data, @password, @db)
      end

    end

  end
end
