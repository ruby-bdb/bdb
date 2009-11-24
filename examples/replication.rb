require 'rubygems'
require 'bdb/database'
require 'fileutils'

role = ARGV.shift || 'client'

MASTER = 'localhost:8888'
CLIENT = 'localhost:9999'

DIR = "/tmp/#{role}"
FileUtils.rmtree DIR
FileUtils.mkdir  DIR

puts "Starting #{role}..."

Bdb::Environment.replicate DIR, :from => MASTER, :to => CLIENT, :host => role == 'master' ? MASTER : CLIENT
db = Bdb::Database.new('foo', :path => DIR)

loop do
  100.times do |i|
    if role == 'master'
      db.set(i, db[i].to_i + 1)
      puts "#{i}: #{db[i]}"
    else
      puts "#{i}: #{db[i]}"
    end
    sleep 0.1
  end
end
