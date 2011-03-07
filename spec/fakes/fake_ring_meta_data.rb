class FakeRingMetaData

  attr_accessor :ring_size
  attr_accessor :shards

  def initialize(ring_size)
    @ring_size = ring_size
    @shards = {}
  end

  def shard(shard_no)
    return @shards[shard_no]
  end

end
