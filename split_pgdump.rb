#!/usr/bin/env ruby
require 'optparse'
require 'fileutils'
require 'strscan'

rules_file = 'split.rules'
output_file = 'dump.sql'
$debug = false
SORT = `which sort`.chomp

opts = OptionParser.new
opts.on("-r", "--rules=RULES_FILE", "File with rules on table splitting (default 'split.rules')") {|v| rules_files = v}
opts.on("-f", "--file=FILE", "main file name (default 'dump.sql').", "Table content will be storred in FILE-tables directory") {|v| output_file = v}
opts.on("-d", "--debug", "debug"){|v| $debug = true}
opts.on_tail("--help", "this message"){|v| puts opts; exit}

opts.parse(ARGV)

tables_dir = output_file + '-tables'

FileUtils.rm_f output_file
FileUtils.rm_rf Dir[File.join(tables_dir, '*')]
FileUtils.mkdir_p tables_dir

class Rule
  class ParseError < StandardError; end

  attr_reader :regex, :split_parts, :sort_keys
  def self.parse(line)
    line = line.sub(%r{(;|#|//).*$},'').strip
    return if line.empty?

    if line =~ /^(\S+)(?:\s+split:(\S+))?(?:\s+sort:((?:(?:[^\s:]+)(?::[MbdfghinRrV]+)?(?:\s+|\s*$))+))?$/
      puts "#$1 split:#$2 sort:#$3" if $debug
      new($1, $2, $3)
    else
      raise ParseError, "Wrong rule line #{line}"
    end
  end

  def initialize(table_regex, split_expr, sort_keys)
    @regex = Regexp.new table_regex
    parse_split_expr(split_expr)
    parse_sort_keys(sort_keys)
  end

  def parse_split_expr(split_expr)
    s = StringScanner.new(split_expr || '')
    parts = []
    while !s.eos?
      if field = s.scan(/\$[^\[%]+/)
        field = field[1..-1]
        if range = s.scan(/\[[+-]?\d+\.\.\.?[+-]?\d+\]/)
          parts << {:type=> :field, :field=> field, :range => range[1..-2]}
        elsif mod = s.scan(/%\d+/)
          parts << {:type=> :field, :field => field, :mod => mod[1..-1]}
        end
        if sep = s.scan(/![^$\s#\\]*/)
          if sep > '!'
            parts << {:type => :sep, :sep => sep[1..-1]}
          end
          next
        end
      end
      raise ParseError, "Wrong format of split expr #{split_expr} (rest: #{s.rest})"
    end
    @split_parts = parts
  end

  def parse_sort_keys(sort_keys)
    @sort_keys = (sort_keys || '').scan(/([^\s:]+)(?::([MbdfghinRrV]+))?/).map do |key, flags|
      {:field => key, :flags => flags}
    end
  end
end

Rules = []
if File.exists?(rules_file)
  File.open(rules_file) do |f|
    f.each_line do |line|
      if rule = Rule.parse(line)
        Rules << rule
      end
    end
  end
end

def Rules.find_rule(table_name)
  find{|rule| table_name =~ rule.regex}
end

class Table
  class NoColumn < StandardError; end
  ONE_FILE_CACHE_SIZE = 128 * 1024
  TOTAL_CACHE_SIZE = 5 * 1024 * 1024
  class OneFile
    attr_reader :file_name, :cache_size
    def initialize(dir, name)
      @file_name = File.join(dir, name)
      @cache_lines = []
      @cache_size = 0
    end

    def add_line(line)
      @cache_lines << line
      @cache_size += line.size
      flush if @cache_size > ONE_FILE_CACHE_SIZE
    end

    def flush
      dir = File.dirname(@file_name)
      unless File.directory?(dir)
        FileUtils.mkdir_p(dir)
      end
      File.open(@file_name, 'a') do |f|
        @cache_lines.each{|l| f.write(l)}
      end
      @cache_lines.clear
      @cache_size = 0
    end

    def write_finish
      File.open(@file_name, 'a') do |f|
        f.puts('\\.')
      end
    end

    def sort(sort_line = [])
      args = [SORT]
      if sort_line && !sort_line.empty?
        args.concat sort_line
      else
        args << '-n'
      end
      args.push '-o', @file_name, @file_name
      puts args.join(' ')  if $debug
      system *args
    end
  end

  attr_reader :name, :columns, :files, :sort_line
  def initialize(dir, name, columns)
    @dir = dir
    @table = name
    @columns = columns.map{|c| c.sub(/^"(.+)"$/, '\\1')}
    if @rule = Rules.find_rule(name)
      apply_rule
    else
      @split_args = []
    end
    @files = {}
    @total_cache_size = 0
  end

  def apply_rule
    split_string = ''
    @rule.split_parts.each do |part|
      case part[:type]
      when :sep
        split_string << part[:sep]
      when :field
        i = @columns.find_index(part[:field])
        raise NoColumn, part[:field]  unless i
        field = "values[#{i}]"
        if part[:mod]
          mod_s = part[:mod]
          mod = mod_s.to_i
          split_string << "\#{'%0#{mod_s.size}d' % (#{field}.to_i / #{mod} * #{mod})}"
        else
          if part[:range]
            field += "[#{part[:range]}]"
          end
          split_string << "\#{#{field}\}"
        end
      end
    end

    eval <<-"EOF"
      def self.file_name(values)
        name = %{#{split_string}}.gsub(/\\.\\.|\\s|\\?|\\*/, '_')
        "\#@table/\#{name}.dat"
      end
    EOF

    @sort_args = @rule.sort_keys.map do |key|
      i = @columns.find_index(key[:field])
      raise NoColumn, key[:field]  unless i
      i += 1
      "--key=#{i},#{i}#{key[:flags]}"
    end
  end

  def file_name(values)
    "#@table.dat"
  end

  def add_line(line)
    values = line.chomp.split("\t")
    fname = file_name(values)
    one_file = @files[fname] ||= OneFile.new(@dir, fname)
    @total_cache_size -= one_file.cache_size
    one_file.add_line(line)
    @total_cache_size += one_file.cache_size
    flush_all if @total_cache_size > TOTAL_CACHE_SIZE
  end

  def flush_all
    @files.each{|name, one_file| one_file.flush}
    @total_cache_size = 0
  end

  def copy_lines
    if block_given?
      @files.each do |name, one_file|
        yield "\\copy #{@table} (#{@columns.join(', ')}) from #{one_file.file_name}"
      end
    else
      Enumerator.new(self, :copy_lines)
    end
  end

  def finish_all
    @files.each do |name, one_file|
      one_file.sort(@sort_args)
      one_file.write_finish
    end
  end
end

state = :schema
dump_lines, table = [], nil
tables = []

File.open(output_file, 'w') do |out|
  STDIN.each_line do |line|
    case state
    when :schema
      if line =~ /^COPY (\w+) \(([^)]+)\) FROM stdin;/
        table_name, columns = $1, $2.split(', ')
        table = Table.new(tables_dir, table_name, columns)
        tables << table
        puts "Start to write table #{table_name}" if $debug
        state = :table
      else
        out.write line
      end
    when :table
      if line =~ /^\\\.[\r\n]/
        table.flush_all
        table.copy_lines.each{|l| out.puts l}
        table = nil
        state = :schema
      else
        table.add_line(line)
      end
    end
  end
end

tables.each{|table| table.finish_all}
