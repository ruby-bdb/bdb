require 'bdb/base'

class Bdb::PartitionedDatabase < Bdb::Base
  SEPARATOR = '__'
  PARTITION_PATTERN = /^[-\w]*$/

  def initialize(base_name, opts = {})
    @base_name    = base_name
    @partition_by = opts.delete(:partition_by)
    super(opts)
  end
  attr_reader :base_name, :partition_by, :partition

  def databases
    @databases ||= {}
  end

  def database(partition = nil)
    partition ||= self.partition
    raise 'partition value required' if partition.nil?
    partition = partition.to_s
    raise "invalid partition value: #{partition}" unless partition =~ PARTITION_PATTERN

    databases[partition] ||= begin
      name = [partition, base_name].join(SEPARATOR)
      database = Bdb::Database.new(name, config)
      indexes.each do |field, opts|
        database.index_by(field, opts)
      end
      database
    end
  end

  def partitions
    Dir[environment.path + "/*#{SEPARATOR}#{base_name}"].collect do |file|
      File.basename(file).split(SEPARATOR).first
    end
  end

  def with_partition(partition)
    @partition, old_partition = partition, @partition
    yield
  ensure
    @partition = old_partition
  end

  def close
    databases.each do |partition, database|
      database.close
    end
    @databases.clear
  end

  def get(*keys, &block)
    opts = keys.last.kind_of?(Hash) ? keys.last : {}
    database(opts[partition_by]).get(*keys, &block)
  end

  def set(key, value, opts = {})
    partition = get_field(partition_by, value)
    database(partition).set(key, value, opts)
  end

  def delete(key, opts = {})
    database(opts[partition_by]).delete(key)
  end

  # Deletes all records in the database. Beware!
  def truncate!
    partitions.each do |partition|
      database(partition).truncate!
    end
  end
end
