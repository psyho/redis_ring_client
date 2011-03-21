require File.expand_path('../../../spec_helper', __FILE__)

describe RedisRing::Client::RingProxy do

  def readable_params(params)
    translated = params.map do |type, name|
      case type
      when :req then name.to_s
      when :rest then "*#{name}"
      when :block then "&#{name}"
      when :opt then "#{name} = some_value"
      else
        raise "Unknown parameter type #{type}"
      end
    end

    return translated.join(", ")
  end

  it "should have the same public interface as Redis" do
    difference = Redis.public_instance_methods - RedisRing::Client::RingProxy.public_instance_methods

    ignored = [:client, :id, :method_missing]

    difference -= ignored

    unless difference == []
      puts "#{difference.size} missing methods:"

      difference.sort.each do |method_name|
        puts "#{method_name}(#{readable_params(Redis.instance_method(method_name).parameters)})"
      end

      fail("Not all methods implemented")
    end
  end

  context "with real RedisRing" do
    before(:all) do
      @proxy = RedisRing::Client::RingProxy.new
    end

    before(:each) do
      @proxy.flushdb
    end

    describe "single key operations" do
      it "should have simple key operations implemented" do
        @proxy.set('foo', 1)

        @proxy.get('foo').should == '1'
      end

      it "should have string operations implemented" do
        3.times { @proxy.append('foo', 'bar') }
        @proxy.incr('bar')
        @proxy.incrby('bar', 10)
        @proxy.decrby('bar', 3)

        @proxy.strlen('foo').should == 9
        @proxy.get('bar').should == '8'
      end

      it "should have list operations implemented" do
        3.times { |n| @proxy.lpush('foo', "bar#{n}") }

        @proxy.llen('foo').should == 3
        @proxy.lpop('foo').should == 'bar2'
        @proxy.rpop('foo').should == 'bar0'
      end

      it "should have set operations implemented" do
        3.times { |n| @proxy.sadd('foo', "bar#{n}") }

        @proxy.scard('foo').should == 3
        @proxy.smembers('foo').sort.should == ['bar0', 'bar1', 'bar2']
      end

      it "should have zset operations implemented" do
        @proxy.zadd 'foo', 1, 'bar'
        @proxy.zadd 'foo', 3, 'baz'
        @proxy.zadd 'foo', 2, 'bam'

        @proxy.zcard('foo').should == 3
        @proxy.zcount('foo', 1, 2).should == 2
        @proxy.zrange('foo', 0, -1).should == ['bar', 'bam', 'baz']
        @proxy.zrange('foo', 0, -1, :with_scores => true).should == ['bar', '1', 'bam', '2', 'baz', '3']
      end

      it "should have hash operations implemented" do
        @proxy.hset 'foo', 'bar', 'hello'
        3.times { @proxy.hincrby 'foo', 'baz', 2 }

        @proxy.hkeys('foo').should == ['bar', 'baz']
        @proxy.hgetall('foo').should == {'bar' => 'hello', 'baz' => '6'}
      end
    end

    describe "multi key operations" do
      it "should allow multi key operations as long as they operate on the same shard" do
        @proxy.mset 'foo{one}', 1, 'bar{one}', 2

        @proxy.mget('foo{one}', 'bar{one}').should == ['1', '2']
      end

      it "should raise an exception if running an operation on multiple shards" do
        lambda{
          @proxy.mapped_mset 'foo{one}' => 1, 'bar{two}' => 2
        }.should raise_exception(RedisRing::Client::MultiShardOperationError)
      end

      it "should work with multi key zset operations" do
        @proxy.zadd 'foo{one}', 1, 1
        @proxy.zadd 'foo{one}', 1, 2
        @proxy.zadd 'foo{one}', 1, 3

        @proxy.zadd 'bar{one}', 1, 2
        @proxy.zadd 'bar{one}', 1, 3
        @proxy.zadd 'bar{one}', 1, 4

        @proxy.zinterstore 'baz{one}', ['foo{one}', 'bar{one}']

        @proxy.zrange('baz{one}', 0, -1).should == ['2', '3']
      end
    end

    describe "scather-gather operations" do
      it "should sum dbsize and similar operations" do
        keys = %w{ala ma kota kot ali nazywa sie as}
        keys.each_with_index { |key, idx| @proxy.set(key, idx) }

        @proxy.keys('*').sort.should == keys.sort
        @proxy.dbsize.should == keys.size
      end

      it "should return last_result for operations where result is not important" do
        @proxy.bgsave.should == "Background saving started"
        @proxy.flushdb.should == "OK"
      end
    end

    describe "unsupported operations" do
      it "should raise an exception when using an operation with undefined behavior in RedisRing" do
        lambda {
          @proxy.multi do
            @proxy.set 'foo{one}', 1
            @proxy.set 'foo{two}', 2
          end
        }.should raise_exception(RedisRing::Client::UnsupportedOperationError)
      end
    end

    describe "random shard operations" do
      it "should execute the operation on a random shard" do
        @proxy.ping.should == "PONG"
      end
    end

  end

end
