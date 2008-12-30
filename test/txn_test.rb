require 'test_helper'

class TxnTest < Test::Unit::TestCase

  def setup
    mkdir File.join(File.dirname(__FILE__), 'tmp')
    @env = Bdb::Env.new(0)
    env_flags = Bdb::DB_CREATE |    # Create the environment if it does not already exist.
                 Bdb::DB_INIT_TXN  | # Initialize transactions
                 Bdb::DB_INIT_LOCK | # Initialize locking.
                 Bdb::DB_INIT_LOG  | # Initialize logging
                 Bdb::DB_INIT_MPOOL  # Initialize the in-memory cache.
    @env.open(File.join(File.dirname(__FILE__), 'tmp'), env_flags, 0);
    
    @db = @env.db
    @db.open(nil, 'db1.db', nil, Bdb::Db::BTREE, Bdb::DB_CREATE | Bdb::DB_AUTO_COMMIT, 0)    
  end
  
  def teardown
    @db.close(0)
    @env.close
    rm_rf File.join(File.dirname(__FILE__), 'tmp')
  end

  def test_commit
    txn = @env.txn_begin(nil, 0)
    @db.put(txn, 'key', 'value', 0)
    txn.commit(0)
    assert_equal 'value', @db.get(nil, 'key', nil, 0)
  end

  def test_abort
    txn = @env.txn_begin(nil, 0)
    @db.put(txn, 'key', 'value', 0)
    txn.abort
    assert_nil @db.get(nil, 'key', nil, 0)
  end

  def test_id
    txn = @env.txn_begin(nil, 0)
    @db.put(txn, 'key', 'value', 0)
    assert txn.tid
    txn.commit(0)
    assert_equal 'value', @db.get(nil, 'key', nil, 0)
  end

  def test_timeout
    txn = @env.txn_begin(nil, 0)
    txn.set_timeout(10, Bdb::DB_SET_TXN_TIMEOUT)
    @db.put(txn, 'key', 'value', 0)
    txn.commit(0)
    assert_equal 'value', @db.get(nil, 'key', nil, 0)
  end

  def test_stat
    txn = @env.txn_begin(nil, 0)
    @db.put(txn, 'key', 'value', 0)
    txn.commit(0)
    txn_stat = @env.txn_stat(0)
    assert txn_stat
    assert txn_stat['st_ncommits'] > 0
    assert_equal 'value', @db.get(nil, 'key', nil, 0)    
  end

  def test_stat_active
    txn = @env.txn_begin(nil, 0)
    @db.put(txn, 'key', 'value', 0)
    txn_stat = @env.txn_stat(0)
    txn.commit(0)
    assert_equal 1, txn_stat['st_txnarray'].length
    assert_equal 'value', @db.get(nil, 'key', nil, 0)    
  end

end
