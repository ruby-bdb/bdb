require File.dirname(__FILE__) + '/database_test_helper'

MASTER_DIR = '/tmp/bdb_rep_test_master'
CLIENT_DIR = '/tmp/bdb_rep_test_client'
FileUtils.rmtree MASTER_DIR; FileUtils.mkdir MASTER_DIR; 
FileUtils.rmtree CLIENT_DIR; FileUtils.mkdir CLIENT_DIR

MASTER = 'localhost:8888'
CLIENT = 'localhost:8889'

N = 100

class ReplicationTest < Test::Unit::TestCase
  def setup
    Bdb::Environment.replicate CLIENT_DIR, :from => MASTER, :to => CLIENT, :host => CLIENT #, :verbose => true
    
    @pid = Process.fork do
      Bdb::Environment.replicate MASTER_DIR, :from => MASTER, :to => CLIENT, :host => MASTER #, :verbose => true
      db = Bdb::Database.new('foo', :path => MASTER_DIR)

      N.times do |i|
        db.set(i, i + 1)
        log "m"
        sleep 0.1
      end
      sleep 10
    end
  end
  
  def teardown
    Process.kill(9, @pid)
  end

  def test_single_process_replication
    db = Bdb::Database.new('foo', :path => CLIENT_DIR)
    N.times do |i|
      sleep 0.2
      assert_equal i + 1, db[i]
      log "c"
    end
  end

  def log(action)
    print action
    $stdout.flush
  end
end
