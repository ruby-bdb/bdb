class Bdb::ResultSet
  class LimitReached < Exception; end
  
  def initialize(opts, &block)
    @block  = block
    @count  = 0
    @limit  = opts[:limit] || opts[:per_page]
    @limit  = @limit.to_i if @limit
    @offset = opts[:offset] || (opts[:page] ? @limit * (opts[:page] - 1) : 0)
    @offset = @offset.to_i if @offset
    
    if @group = opts[:group]
      raise 'block not supported with group' if @block     
      @results = hash_class.new
    else
      @results = []
    end
  end
  attr_reader :count, :group, :limit, :offset, :results
  
  def hash_class
    @hash_class ||= defined?(ActiveSupport::OrderedHash) ? ActiveSupport::OrderedHash : Hash
  end
  
  def <<(item)
    @count += 1
    return if count <= offset
    
    raise LimitReached if limit and count > limit + offset
    
    if group
      key = item.bdb_locator_key
      group_key = group.is_a?(Fixnum) ? key[0,group] : key
      (results[group_key] ||= []) << item
    elsif @block
      @block.call(item)
    else
      results << item
    end
  end
end
