# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "redis_ring/client/version"

Gem::Specification.new do |s|
  s.name        = "redis_ring_client"
  s.version     = RedisRing::Client::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Adam Pohorecki"]
  s.email       = ["adam@pohorecki.pl"]
  s.homepage    = "http://github.com/psyho/redis_ring_client"
  s.summary     = %q{Client for RedisRing}
  s.description = %q{The client counterpart to the RedisRing gem.}

  s.rubyforge_project = "redis_ring_client"

  s.add_dependency 'redis'
  s.add_dependency 'json'

  s.add_development_dependency 'rspec'
  s.add_development_dependency 'mocha'
  s.add_development_dependency 'ruby-prof'

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end
