require File.expand_path('../../../spec_helper', __FILE__)

describe RedisRing::Client::RingMetaData do

  def sample_shards_hash
    {
      :ring_size => 10,
      :shards => {
        0 => {:host => '192.168.1.1', :port => 6401, :status => :running},
        1 => {:host => '192.168.1.1', :port => 6402, :status => :running},
        2 => {:host => '192.168.1.1', :port => 6403, :status => :running},
        3 => {:host => '192.168.1.1', :port => 6404, :status => :running},
        4 => {:host => '192.168.1.1', :port => 6405, :status => :running},
        5 => {:host => '192.168.1.1', :port => 6406, :status => :running},
        6 => {:host => '192.168.1.1', :port => 6407, :status => :running},
        7 => {:host => '192.168.1.1', :port => 6408, :status => :running},
        8 => {:host => '192.168.1.1', :port => 6409, :status => :running},
        9 => {:host => '192.168.1.1', :port => 6410, :status => :running}
      }
    }
  end

  def sample_shard_json
    sample_shards_hash.to_json
  end

  def stub_zookeeper
    Zookeeper.any_instance.expects(:get).returns(:data => sample_shard_json)
  end

  it "should download json lazily" do
    @metadata = RedisRing::Client::RingMetaData.new('localhost:2181', 'some-name')

    stub_zookeeper

    @metadata.ring_size.should == 10
  end

  context "with sample shards json" do
    before(:each) do
      stub_zookeeper

      @metadata = RedisRing::Client::RingMetaData.new('localhost:2181', 'some-name')
    end

    it "should have ring_size of 10" do
      @metadata.ring_size.should == 10
    end

    it "should have 10 shards" do
      10.times do |n|
        @metadata.shard(n).host.should == '192.168.1.1'
        @metadata.shard(n).port.should == 6401 + n
        @metadata.shard(n).status.should == :running
      end
    end

    it "should raise an exception when trying to acces unexisting shard metadata" do
      lambda {
        @metadata.shard(10)
      }.should raise_exception(RedisRing::Client::UnknownShardError)
    end
  end
end
