require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "dk-bdb"
    gem.summary = %Q{Ruby Berkeley DB}
    gem.description = %Q{Advanced Ruby Berkeley DB library.}
    gem.email = %w[code@justinbalthrop.com Denis.Knauf@gmail.com]
    gem.homepage = "http://github.com/DenisKnauf/bdb"
    gem.authors = ["Justin Balthrop", "Denis Knauf"]
    gem.files = %w[AUTHORS README.md VERSION ext/*.c ext/*.h lib/**/*.rb test/*.rb]
    gem.extensions = %w[ext/extconf.rb]
    gem.require_paths = %w[ext lib]
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: sudo gem install jeweler"
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test' << 'ext'
  test.pattern = 'test/**/*_test.rb'
  test.verbose = true
end

begin
  require 'rcov/rcovtask'
  Rcov::RcovTask.new do |test|
    test.libs << 'test'
    test.pattern = 'test/**/*_test.rb'
    test.verbose = true
  end
rescue LoadError
  task :rcov do
    abort "RCov is not available. In order to run rcov, you must: sudo gem install spicycode-rcov"
  end
end

task :test => :check_dependencies

task :default => :test

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  if File.exist?('VERSION')
    version = File.read('VERSION')
  else
    version = ""
  end

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "bdb #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
