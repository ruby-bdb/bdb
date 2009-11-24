require 'bdb'
require 'tuple'
require 'bdb/environment'
require 'bdb/result_set'

class Bdb::Base
  def initialize(opts)
    @config  = Bdb::Environment.config.merge(opts)
    @indexes = {}
  end
  attr_reader :indexes
  
  def config(config = {})
    @config.merge!(config)
  end
    
  def index_by(field, opts = {})
    raise "index on #{field} already exists" if indexes[field]
    indexes[field] = opts
  end
  
  def path
    config[:path] || Dir.pwd
  end

  def environment
    @environment ||= Bdb::Environment.new(path, self)
  end
  
  def transaction(nested = true, &block)
    environment.transaction(nested, &block)
  end
  
  def synchronize(&block)
    environment.synchronize(&block)
  end
  
  def checkpoint(opts = {})
    environment.checkpoint(opts)
  end

  def master?
    environment.master?
  end

private

  def get_field(field, value)
    value.kind_of?(Hash) ? value[field] : value.send(field)
  end
end

class Object
  attr_accessor :bdb_locator_key
end

# Array comparison should try Tuple comparison first.
class Array
  cmp = instance_method(:<=>)

  define_method(:<=>) do |other|
    begin
      Tuple.dump(self) <=> Tuple.dump(other)
    rescue TypeError => e
      cmp.bind(self).call(other)
    end
  end
end
