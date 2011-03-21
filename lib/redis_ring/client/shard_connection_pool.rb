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
        connection, conn_id = @connections[shard_number]
        shard_metadata = metadata.shard(shard_number)
        unless conn_id == shard_metadata.to_sym
          connection = new_connection(shard_metadata.host, shard_metadata.port, db, password)
          conn_id = shard_metadata.to_sym
          @connections[shard_number] = [connection, conn_id]
        end
        connection
      end

      protected

      def new_connection(host, port, db, password)
        Redis.new(:host => host, :port => port, :db => db, :password => password)
      end

    end

  end
end
