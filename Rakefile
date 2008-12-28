require 'rubygems'
require 'rake/gempackagetask'
require 'rake/rdoctask'
require 'rake/testtask'

load 'bdb.gemspec'
 
Rake::GemPackageTask.new(BDB_SPEC) do |pkg|
    pkg.need_tar = true
end
 
task :default => "test"

desc "Clean"
task :clean do
  include FileUtils
  rm_rf File.join('ext', 'bdb_aux._c')
  rm_rf File.join('ext', 'Makefile')
  rm_rf File.join('ext', 'mkmf.log')
  rm_rf File.join('ext', 'conftest.c')
  rm_rf File.join('ext', '*.o')
  rm_rf 'pkg'
end
 
desc "Run tests"
Rake::TestTask.new("test") do |t|
  t.libs << ["test", "ext"]
  t.pattern = 'test/*_test.rb'
  t.verbose = true
  t.warning = true
end
 
task :doc => [:rdoc]
namespace :doc do
  Rake::RDocTask.new do |rdoc|
    files = ["README", "lib/**/*.rb"]
    rdoc.rdoc_files.add(files)
    rdoc.main = "README.textile"
    rdoc.title = "Bdb Docs"
    rdoc.rdoc_dir = "doc"
    rdoc.options << "--line-numbers" << "--inline-source"
  end
end
