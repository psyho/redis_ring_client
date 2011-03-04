require File.expand_path('../../../spec_helper', __FILE__)

describe RedisRing::Client::Sharder do

  class FakeMetaData
    attr_accessor :ring_size

    def initialize(ring_size)
      @ring_size = ring_size
    end
  end

  before(:each) do
    @sharder = RedisRing::Client::Sharder.new(@meta_data = FakeMetaData.new(1024))
  end

  it "should hash the same value always to the same shard" do
    shards = (0..9).map{|n| @sharder.shard_for_key("some_key")}

    shards.uniq.size.should == 1
  end

  it "should never return less than 0 or more than ring_size - 1" do
    ['foo', 'bar', 'baz'].product(['0', '1', '2']).product(['a', 'b', 'c']).each do |arr|
      str = arr.flatten.join('_')
      shard = @sharder.shard_for_key(str)

      shard.should >= 0
      shard.should < 1024
    end
  end

  it "should be sensitive to ring_size change" do
    old_val = @sharder.shard_for_key('foo')
    @meta_data.ring_size = 100
    @sharder.shard_for_key('foo').should_not == old_val
  end

  it "should return different shards for slightly different values" do
    @sharder.shard_for_key('foo1').should_not == @sharder.shard_for_key('foo2')
  end

  it "should take advantage of the {shard} specifier" do
    @sharder.shard_for_key('foo1{one}').should == @sharder.shard_for_key('foo2{one}')
    @sharder.shard_for_key('foo1{one}').should == @sharder.shard_for_key('one')
  end

end
