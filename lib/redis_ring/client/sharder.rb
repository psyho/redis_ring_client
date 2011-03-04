module RedisRing
  module Client

    class Sharder

      attr_reader :metadata

      def initialize(metadata)
        @metadata = metadata
      end

      def shard_for_key(key)
        crc = Zlib.crc32(hashable_part(key.to_s))
        return crc % metadata.ring_size
      end

      private

      def hashable_part(key)
        if key =~ /{([^}]*)}$/
          return Regexp.last_match(1)
        else
          return key
        end
      end

    end

  end
end
