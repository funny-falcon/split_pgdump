#!/usr/bin/env ruby
require 'optparse'
require 'fileutils'

rules_file = 'split.rules'
output_file = 'dump.sql'
$debug = false
pg_dump = [`which pg_dump`.chomp]

opts = OptionParser.new
opts.on("-r", "--rules=RULES_FILE", "File with rules on table splitting (default 'split.rules')") {|v| rules_files = v}
opts.on("-f", "--file=FILE", "main file name (default 'dump.sql').", "Table content will be storred in FILE-tables directory") {|v| output_file = v}
opts.on("-F", "--format=c|t|p", "output file format is ignored, only plain text is used"){|v| }
opts.on("-d", "--debug", "debug"){|v| $debug = true}
opts.on("-Z", "--compress=0-9", Integer, "compression level ignored"){|v| }
opts.on("-s", "--schema-only", "split_pgdump usage with `schema_only` is meaningless, but you allowed to do it"){|v| pg_dump << "-s" }
opts.on("--inserts", "--column-inserts", "sorry, could not work with inserts, ignored"){|v| }
opts.on_tail("--help", "this message"){|v| puts opts; exit}
# following options are passed to pg_dump directly
add_opts = <<EOF
  -a, --data-only             dump only the data, not the schema
  -b, --blobs                 include large objects in dump
  -c, --clean                 clean (drop) database objects before recreating
  -C, --create                include commands to create database in dump
  -E, --encoding=ENCODING     dump the data in encoding ENCODING
  -n, --schema=SCHEMA         dump the named schema(s) only
  -N, --exclude-schema=SCHEMA do NOT dump the named schema(s)
  -o, --oids                  include OIDs in dump
  -O, --no-owner              skip restoration of object ownership
  -S, --superuser=NAME        superuser user name to use in plain-text format
  -t, --table=TABLE           dump the named table(s) only
  -T, --exclude-table=TABLE   do NOT dump the named table(s)
  -x, --no-privileges         do not dump privileges (grant/revoke)
  --binary-upgrade            for use by upgrade utilities only
  --disable-dollar-quoting    disable dollar quoting, use SQL standard quoting
  --disable-triggers          disable triggers during data-only restore
  --no-tablespaces            do not dump tablespace assignments
  --role=ROLENAME             do SET ROLE before dump
  --use-set-session-authorization  use SET SESSION AUTHORIZATION commands instead of ALTER OWNER commands to set ownership
  -h, --host=HOSTNAME      database server host or socket directory
  -p, --port=PORT          database server port number
  -U, --username=NAME      connect as specified database user
  -w, --no-password        never prompt for password
  -W, --password           force password prompt (should happen automatically)
EOF
add_opts.each_line do |line|
  if line =~ /(?:(-\w), )?(--\S+)\s+(.+)/
    args = [$1, $2, $3]
    if args[1] =~ /=[A-Z]/
      block = lambda{|v| pg_dump << args[0] << v}
    else
      block = lambda{ pg_dump << args[0] }
    end
    args.compact!
    opts.on(*args, &block)
  end
end

pg_dump.concat opts.parse(ARGV)
p pg_dump  if $debug

tables_dir = output_file + '-tables'

FileUtils.rm_f output_file
FileUtils.rm_rf Dir[File.join(tables_dir, '*')]
FileUtils.mkdir_p tables_dir

state = :schema
table, columns = nil, nil
table_file_name, table_file = nil, nil
IO.popen(pg_dump) do |dump|
  File.open(output_file, 'w') do |out|
    for line in dump
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
        table_file.write(line)
        if line[0,2] == '\.'
          table_file.close
          out.write "\\copy #{table} (#{columns.join(', ')}) from #{table_file_name}"
          state = :schema
          table, columns = nil, nil
          table_file_name = nil
          table_file = nil
        end
      end
    end
  end
end

