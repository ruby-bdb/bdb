require 'test_helper'
require File.dirname(__FILE__) + '/../lib/bdb/simple'

class SimpleTest < Test::Unit::TestCase
  def setup
    mkdir File.join(File.dirname(__FILE__), 'tmp')
 
    file = File.join(File.dirname(__FILE__), 'tmp', 'simple.db')
    @db  = Bdb::Simple.new(file)

    file = File.join(File.dirname(__FILE__), 'tmp', 'simple-dup.db')
    @dbd = Bdb::Simple.new(file, :dup => true)
  end
  
  def teardown
    @db.close
    @dbd.close
    rm_rf File.join(File.dirname(__FILE__), 'tmp')
  end
  
  def test_put_and_get
    @db['key'] = 'data'
    assert_equal 'data', @db['key']

    @dbd['key'] = 'data1'
    @dbd['key'] = 'data2'
    assert_equal ['data1', 'data2'], @dbd['key'].to_a
  end
  
  def test_delete
    @db['key'] = 'data'
    assert_equal 'data', @db['key']

    @db.delete('key')    
    assert_nil @db['key']
  end
  
  def test_range
    (1..10).each {|i| @db[i] = "data#{i}"}
    
    assert_equal (3..7).collect {|i| "data#{i}"}, @db[3..7].to_a
  end

  def test_compare_absolute
    list = [5, 6, "foo", :bar, "bar", :foo, [1,2,4], true, [1,2,3], false, [1], [2], nil, {}, {:b => 1, :a => 1}, {:b => 2, :a => 1}]

    expected = [nil, true, :bar, :foo, "bar", "foo", 5, 6, false, [1], [1, 2, 3], [1, 2, 4], [2], {}, {:a=>1, :b=>1}, {:a=>1, :b=>2}]
    assert_equal expected, list.sort {|a,b| Bdb::Simple.compare_absolute(a,b)}
    100.times do
      assert_equal expected, list.shuffle.sort {|a,b| Bdb::Simple.compare_absolute(a,b)}
    end
  end
end
