require 'test_helper'
require File.dirname(__FILE__) + '/../lib/bdb/simple'

class SimpleTest < Test::Unit::TestCase
  def setup
    @path = File.join(File.dirname(__FILE__), 'tmp')
    rm_rf @path
    mkdir @path
    open
  end
  
  def teardown
    close
    rm_rf @path
  end
  
  def open
    @db  = Bdb::Simple.new(@path)
    @dbd = Bdb::Simple.new(@path, :name => 'dup', :dup => true)
  end

  def close
    @db.close
    @dbd.close
  end

  def test_put_and_get
    @db['key'] = 'data'
    assert_equal 'data', @db['key']

    @dbd['key'] = 'data1'
    @dbd['key'] = 'data2'
    assert_equal ['data1', 'data2'], @dbd['key'].to_a
  end
  
  def test_update
    @db[:key] = 0
    close

    pids = []
    5.times do
      pids << Process.fork do
        db = Bdb::Simple.new(@path)
        10.times do
          db.update(:key) do |v|
            sleep(0.1)
            v + 1
          end
        end
        db.close
      end
    end
    pids.each {|pid| Process.wait(pid)}

    open
    assert_equal 50, @db[:key]
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

    expected = [nil, false, true, 5, 6, :bar, :foo, "bar", "foo", [1], [1, 2, 3], [1, 2, 4], [2], {}, {:a=>1, :b=>1}, {:a=>1, :b=>2}]
    assert_equal expected, list.sort {|a,b| Bdb::Simple.compare_absolute(a,b)}
    100.times do
      assert_equal expected, list.shuffle.sort {|a,b| Bdb::Simple.compare_absolute(a,b)}
    end
  end

  def parallel(n)
    threads = []
    n.times do |i|
      threads << Thread.new do
        yield(i)
      end
    end
    threads.each { |thread| thread.join }
  end
end
