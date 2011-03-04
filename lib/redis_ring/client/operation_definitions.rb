module RedisRing
  module Client

    class UnsupportedOperationError < StandardError; end

    module OperationDefinitions

      def self.included(klass)
        klass.send(:include, InstanceMethods)
        klass.extend(ClassMethods)
      end

      module InstanceMethods

        def single_key_operation(name, first_arg, *rest)
          connection = connection_for_key(first_arg)
          return connection.send(name, first_arg, *rest)
        end

        def scather_gather_operation(name, gather, *args, &block)
          raise "Not implemented yet"
        end

        def unsupported_operation(name)
          raise UnsupportedOperationError.new("Operation #{name} is not supported by RedisRing!")
        end

        def random_shard_operation(name, *args)
          raise "Not implemented yet."
        end

        def single_connection_operation(name, keys, *args, &block)
          raise "Not implemented yet."
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

            def #{name}(*args)
              random_shard_operation(:#{name}, *args)
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

        def multi_set_operation(name)
          self.class_eval <<-RUBY

            def #{name}(hash, &block)
              return single_connection_operation(:#{name}, hash.keys, hash, &block)
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
