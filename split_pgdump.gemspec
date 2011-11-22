Gem::Specification.new do |s|
  s.name = 'split_pgdump'
  s.version = '0.3.0'
  s.date = '2011-11-22'
  s.summary = 'split_pgdump is a tool for splitting postgresql dump in a managable set of files'
  s.description = 
    "split_pgdump aimed to produce set of small sorted files from one big dump file.\n"
    "It allows to effectively use SCM tools to store history of data changes and rsync\n"
    "to transfer changes over network"
  s.authors = ["Sokolov Yura aka funny_falcon"]
  s.email  = "funny.falcon@gmail.com"
  s.files   = ["bin/split_pgdump", "README"]
  s.executables << "split_pgdump"
  s.homepage = "https://github.com/funny-falcon/split_pgdump"
  s.licenses = ["GPL"]
end
