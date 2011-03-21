require File.expand_path('../../../spec_helper', __FILE__)

describe RedisRing::Client::ShardConnectionPool do

  before(:each) do
    @metadata = FakeRingMetaData.new(5)
    5.times do |n|
      @metadata.shards[n] = RedisRing::Client::ShardMetaData.new("host#{n}", 666 + n, :running)
    end

    @connection_pool = RedisRing::Client::ShardConnectionPool.new(@metadata, @password = nil, @db = 10)
  end

  it "should create a new connection when there was no shard connection before" do
    @connection_pool.expects(:new_connection).with("host1", 667, @db, @password).returns(:foo).once

    @connection_pool.connection(1).should == :foo
  end

  it "should cache connections" do
    @connection_pool.expects(:new_connection).with("host1", 667, @db, @password).returns(:foo).once

    @connection_pool.connection(1).should == :foo
    @connection_pool.connection(1).should == :foo
  end

  it "should create a new connection if the metadata changes" do
    @connection_pool.expects(:new_connection).with("host1", 667, @db, @password).returns(:foo).once
    @connection_pool.connection(1).should == :foo

    @metadata.shards[1] = RedisRing::Client::ShardMetaData.new("host1", 777, :running)

    @connection_pool.expects(:new_connection).with("host1", 777, @db, @password).returns(:bar).once
    @connection_pool.connection(1).should == :bar
  end

end
