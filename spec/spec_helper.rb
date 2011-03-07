$:.push File.expand_path("../../lib", __FILE__)

require 'redis_ring/client'

require File.expand_path('../fakes/fake_ring_meta_data', __FILE__)

RSpec.configure do |c|
  c.color_enabled = true
  c.mock_with :mocha
end
