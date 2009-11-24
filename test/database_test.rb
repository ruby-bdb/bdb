require File.dirname(__FILE__) + '/database_test_helper'

TMPDIR = File.dirname(__FILE__) + '/tmp'
FileUtils.rmtree TMPDIR
FileUtils.mkdir  TMPDIR

class DatabaseTest < Test::Unit::TestCase
  include DatabaseTestHelper

  def setup
    @db = Bdb::Database.new('foo', :path => TMPDIR)
    @db.truncate!
  end
  
  def teardown
    @db.close
  end
end
