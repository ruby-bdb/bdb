require 'test/unit'
require 'fileutils'
require 'pp'

$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../ext')
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../lib')
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../../tuple/ext')

require 'bdb'
require 'bdb/database'

class Test::Unit::TestCase
  include FileUtils
end
