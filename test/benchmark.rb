require 'rubygems'
require File.dirname(__FILE__) + '/../lib/bdb/simple'
require 'benchmark'

N = 10_000
A = [5, 6, "foo", :bar, "bar", {}, :foo, [1,2,4], true, [1,2,3], false, [1], [2], nil].collect {|i| Marshal.dump(i)}

puts "compare_absolute (#{N} times)"
t = Benchmark.measure do
  N.times do
    A.sort {|a,b| Bdb::Simple.compare_absolute(Marshal.load(a), Marshal.load(b)) }
  end
end
puts t

puts "compare_hash (#{N} times)"
t = Benchmark.measure do
  N.times do
    A.sort {|a,b| Marshal.load(a).hash <=> Marshal.load(b).hash }
  end
end
puts t

puts "compare_raw (#{N} times)"
t = Benchmark.measure do
  N.times do
    A.sort
  end
end
puts t

