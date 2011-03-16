#!/usr/bin/env ruby

$:.push File.expand_path("../../lib", __FILE__)

require 'redis_ring/client'
require 'redis'
require 'ruby-prof'

require 'benchmark'

def single_key_operations(redis, n)
  redis.set("foo-#{n}", n)
  redis.get("foo-#{n}")
  redis.incr("bar-#{n}")
end

def multi_key_operations(redis, n)
  redis.mset("foo-#{n}{x}", n, "bar-#{n}{x}", n)
  redis.mget("foo-#{n}{x}", "bar-#{n}{x}")
end

def with_profiling(profile, &block)
  if profile
    result = RubyProf.profile do
      block.call
    end
    printer = RubyProf::CallTreePrinter.new(result)
    printer.print(File.open("#{profile}.kcg", 'w'))
  else
    block.call
  end
end

count = (ARGV[0] || 10000).to_i
profile = ARGV[1]

redises = {
  :redis_ring => (redis_proxy = RedisRing::Client::RingProxy.new),
  :redis => redis_proxy.connection_for_key("some_key")
}

with_profiling(profile) do
  Benchmark.bmbm do |b|
    [:single_key_operations, :multi_key_operations].each do |method|
      [:redis, :redis_ring].each do |redis_name|
        b.report("#{redis_name} #{method}:") { count.times{|n| send(method, redises[redis_name], n)} }
      end
    end
  end
end
