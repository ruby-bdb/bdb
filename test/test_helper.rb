require 'test/unit'
require 'fileutils'
require File.dirname(__FILE__) + '/../ext/bdb'

class Test::Unit::TestCase
  include FileUtils
end
