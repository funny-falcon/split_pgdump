#!/usr/bin/env ruby
require 'optparse'
require 'fileutils'

rules_file = 'split.rules'
output_file = 'dump.sql'
$debug = false
sort = `which sort`.chomp

opts = OptionParser.new
opts.on("-r", "--rules=RULES_FILE", "File with rules on table splitting (default 'split.rules')") {|v| rules_files = v}
opts.on("-f", "--file=FILE", "main file name (default 'dump.sql').", "Table content will be storred in FILE-tables directory") {|v| output_file = v}
opts.on("-d", "--debug", "debug"){|v| $debug = true}
opts.on_tail("--help", "this message"){|v| puts opts; exit}

tables_dir = output_file + '-tables'

FileUtils.rm_f output_file
FileUtils.rm_rf Dir[File.join(tables_dir, '*')]
FileUtils.mkdir_p tables_dir

state = :schema
table, columns = nil, nil
table_file_name, table_file = nil, nil
File.open(output_file, 'w') do |out|
  STDIN.each_line do |line|
      case state
      when :schema
        if line =~ /^COPY (\w+) \(([^)]+)\) FROM stdin;/
          table, columns = $1, $2.split(', ')
          table_file_name = File.join(tables_dir, table)
          table_file = File.new(table_file_name, 'w')
          puts "Start to write table #{table}" if $debug
          state = :table
        else
          out.write line
        end
      when :table
        if line =~ /^\\\.[\r\n]/
          table_file.close
          puts "Sorting table #{table}" if $debug
          system(sort, '-n', '-o', table_file_name, table_file_name)
          File.open(table_file_name, 'a'){|f| f.write(line)}
          out.write "\\copy #{table} (#{columns.join(', ')}) from #{table_file_name}"
          state = :schema
          table, columns = nil, nil
          table_file_name = nil
          table_file = nil
        else
          table_file.write(line)
        end
      end
  end
end

