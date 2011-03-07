module RedisRing
  module Client

    class ShardConnectionPool

      attr_reader :metadata, :password, :db

      def initialize(metadata, password, db)
        @metadata = metadata
        @password = password
        @db = db
        @connections = {}
      end

      def connection(shard_number)
        @connections[shard_number] ||= new_connection_to_shard(shard_number)
      end

      protected

      def new_connection_to_shard(shard_number)
        shard_metadata = metadata.shard(shard_number)
        new_connection(shard_metadata.host, shard_metadata.port, db, password)
      end

      def new_connection(host, port, db, password)
        Redis.new(:host => host, :port => port, :db => db, :password => password)
      end

    end

  end
end
