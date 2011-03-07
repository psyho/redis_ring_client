module RedisRing
  module Client

    class UnsupportedOperationError < StandardError; end
    class MultiShardOperationError < UnsupportedOperationError; end

    module OperationDefinitions

      def self.included(klass)
        klass.send(:include, InstanceMethods)
        klass.send(:include, GatherOperations)
        klass.extend(ClassMethods)
      end

      module GatherOperations

        def last_result(array)
          return array.last
        end

        def sum(array)
          return array.reduce(:+)
        end

      end

      module InstanceMethods

        def single_key_operation(name, first_arg, *rest)
          connection = connection_for_key(first_arg)
          return connection.send(name, first_arg, *rest)
        end

        def scather_gather_operation(name, gather, *args, &block)
          results = []
          each_connection do |conn|
            results << conn.send(name, *args, &block)
          end
          return send(gather, results)
        end

        def unsupported_operation(name)
          raise UnsupportedOperationError.new("Operation #{name} is not supported by RedisRing!")
        end

        def random_shard_operation(name, *args, &block)
          shard_no = rand(ring_meta_data.ring_size)
          return connection_pool.connection(shard_no).send(name, *args, &block)
        end

        def single_connection_operation(name, keys, *args, &block)
          shard_numbers = keys.map { |key| sharder.shard_for_key(key) }
          unless shard_numbers.uniq.size == 1
            raise MultiShardOperationError.new("Multi-shard atomic operations are not allowed. Try using {shard_secifier} suffix if you really need them. Operation: #{name}, Keys: #{keys.join(', ')}")
          end
          return connection_for_key(keys.first).send(name, *args, &block)
        end

      end

      module ClassMethods

        def single_key_operation(name)
          self.class_eval <<-RUBY

          def #{name}(key, *args)
            return single_key_operation(:#{name}, key, *args)
          end

          RUBY
        end

        def scather_gather_operation(name, gather_function)
          self.class_eval <<-RUBY

          def #{name}(*args, &block)
            return scather_gather_operation(:#{name}, :#{gather_function}, *args, &block)
          end

          RUBY
        end

        def unsupported_operation(name)
          self.class_eval <<-RUBY

            def #{name}(*args)
              unsupported_operation(:#{name})
            end

          RUBY
        end

        def random_shard_operation(name)
          self.class_eval <<-RUBY

            def #{name}(*args, &block)
              random_shard_operation(:#{name}, *args, &block)
            end

          RUBY
        end

        def multi_key_operation(name)
          self.class_eval <<-RUBY

            def #{name}(*keys, &block)
              return single_connection_operation(:#{name}, keys, *keys, &block)
            end

          RUBY
        end

        def mapped_set_operation(name)
          self.class_eval <<-RUBY

            def #{name}(hash, &block)
              return single_connection_operation(:#{name}, hash.keys, hash, &block)
            end

          RUBY
        end

        def regular_set_operation(name)
          self.class_eval <<-RUBY

            def #{name}(*keys_and_values, &block)
              return single_connection_operation(:#{name}, Hash[*keys_and_values].keys, *keys_and_values, &block)
            end

          RUBY
        end

        def multi_zstore_operation(name)
          self.class_eval <<-RUBY

            def #{name}(destination, keys, options = {})
              return single_connection_operation(:#{name}, [destination] + keys, destination, keys, options)
            end

          RUBY
        end

      end

    end
  end
end
