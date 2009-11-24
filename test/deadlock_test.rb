require File.dirname(__FILE__) + '/test_helper'

DIR = '/tmp/bdb_deadlock_test'
Bdb::Environment.config :path => DIR, :cache_size => 1 * 1024 * 1024, :page_size => 512

class DeadlockTest < Test::Unit::TestCase
  def setup
    FileUtils.rmtree DIR
    FileUtils.mkdir  DIR
    Bdb::Environment[DIR].close      
    @db = Bdb::Database.new('foo')
  end

  attr_reader :db

  N = 5000 # total number of records
  R = 10   # number of readers
  W = 10   # number of writers
  T = 20   # reads per transaction
  L = 100  # logging frequency

  def test_detect_deadlock
    pids = []

    W.times do |n|
      pids << fork(&writer)
    end

    sleep(1)

    R.times do
      pids << fork(&reader)
    end

    # Make sure that all processes finish with no errors.
    pids.each do |pid|
      Process.wait(pid)
      assert_equal status, $?.exitstatus
    end
  end

  C = 10
  def test_detect_unclosed_resources
    threads = []

    threads << Thread.new do
      C.times do
        sleep(10)

        pid = fork do
          cursor = db.db.cursor(nil, 0)
          cursor.get(nil, nil, Bdb::DB_FIRST)
          exit!(1)
        end
        puts "\n====simulating exit with unclosed resources ===="
        Process.wait(pid)
        assert_equal 1, $?.exitstatus
      end
    end

    threads << Thread.new do
      C.times do
        pid = fork(&writer(1000))
        Process.wait(pid)
        assert [0,9].include?($?.exitstatus)
      end
    end

    sleep(3)

    threads << Thread.new do
      C.times do
        pid = fork(&reader(1000))
        Process.wait(pid)
        assert [0,9].include?($?.exitstatus)
      end
    end
    
    threads.each {|t| t.join}
  end

  def reader(n = N)
    lambda do
      T.times do
        (1...n).to_a.shuffle.each_slice(T) do |ids|
          db.transaction do
            ids.each {|id| db.get(id)}
          end
          log('r')
        end
      end
      db.close_environment
    end
  end

  def writer(n = N)
    lambda do
      (1...n).to_a.shuffle.each do |id|
        db.transaction do
          begin
            db.set(id, {:bar => "bar" * 1000 + " ayn #{rand}"})
          rescue Bdb::DbError => e
            if e.code == Bdb::DB_KEYEXIST
              db.delete(id)
              retry
            else
              raise(e)
            end
          end
        end
        log('w')
      end
      db.close_environment
    end
  end

  def log(action)
    @count ||= Hash.new(0)
    if @count[action] % L == 0
      print action.to_s
      $stdout.flush
    end
    @count[action] += 1
  end
end
