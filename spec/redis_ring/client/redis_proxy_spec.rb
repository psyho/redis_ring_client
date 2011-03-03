require File.expand_path('../../../spec_helper', __FILE__)

describe RedisRing::Client::RedisProxy do

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
    difference = Redis.public_instance_methods - RedisRing::Client::RedisProxy.public_instance_methods

    unless difference == []
      puts "Missing methods:"

      difference.each do |method_name|
        puts "#{method_name}(#{readable_params(Redis.instance_method(method_name).parameters)})"
      end

      fail("Not all methods implemented")
    end
  end

end
