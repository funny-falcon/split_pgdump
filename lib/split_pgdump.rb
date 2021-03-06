#!/usr/bin/env ruby
# vim: set syntax=ruby shiftwidth=2 softtabstop=2 tabstop=8 expandtab
require 'fileutils'
require 'strscan'
require 'shellwords'

$debug = false

module SplitPgDump
  VERSION = '0.4.0'
end

begin
  require 'split_pgdump/native_compute_name'
rescue LoadError
end

class SplitPgDump::Worker
  attr_accessor :rules_file, :output_file, :sorter, :rules, :num_sorters
  attr_accessor :could_fork, :xargs
  def initialize
    @rules_file = 'split.rules'
    @output_file = 'dump.sql'
    @sorter = `which sort`.chomp
    @xargs = `which xargs`.chomp
    @rules = []
    @num_sorters = 0
    @could_fork = true
  end

  def tables_dir
    output_file + '-tables'
  end

  def clear_files
    FileUtils.rm_f output_file
    FileUtils.rm_rf Dir[File.join(tables_dir, '*')]
    FileUtils.mkdir_p tables_dir
  end

  def parse_rules
    if File.exists?(rules_file)
      File.open(rules_file) do |f|
        f.each_line do |line|
          if rule = SplitPgDump::Rule.parse(line)
            @rules << rule
          end
        end
      end
    else
      puts "NO FILE #{rules_file}"  if $debug
    end
  end

  def find_rule(table)
    @rules.find{|rule| table =~ rule.regex}
  end

  def process_schema_line(out, line)
    if line =~ /^COPY (\w+) \(([^)]+)\) FROM stdin;/
      table_name, columns = $1, $2.split(', ')
      rule = find_rule("#@schema.#{table_name}")
      @table = SplitPgDump::Table.new(tables_dir, @schema, table_name, columns, rule)
      @tables << @table
      puts "Start to write table \t#{table_name}" if $debug
      @start_time = Time.now
      @state = :table
    else
      if line =~ /^SET search_path = ([^,]+)/
        @schema = $1
      end
      out.write line
    end
  end

  def process_copy_line(out, line)
    if line =~ /^\\\.[\r\n]/
      @table.flush_all
      @table.copy_lines{|l| out.puts l}
      puts "Table #{@table.table} copied in \t#{"%.2f" % (Time.now - @start_time)}s" if $debug
      @table = nil
      @state = :schema
    else
      @table.add_line(line)
    end
  end

  def work(in_stream)
    @state = :schema
    @table = nil
    @tables = []
    @schema = 'public'

    File.open(output_file, 'w') do |out|
      in_stream.each_line do |line|
        case @state
        when :schema
          process_schema_line(out, line)
        when :table
          process_copy_line(out, line)
        end
      end
    end

    @start_time = Time.now
    sort_and_finish
    puts "Finished in #{Time.now - @start_time}s #{Process.pid}" if $debug
  end

  def sort_and_finish
    files = []
    for table in @tables
      for one_file in table.files.values
        sort_args = one_file.sort_args(table.sort_args).shelljoin
        files << [one_file, sort_args]
      end
    end
    unless @xargs.empty?
      num_sorters = [@num_sorters, 1].max
      xargs_cmd = [@xargs, '-L1', '-P', num_sorters.to_s, @sorter].shelljoin
      puts xargs_cmd  if $debug
      IO.popen(xargs_cmd, 'w+') do |io|
        files.each{|one_file, sort_args|
          puts sort_args  if $debug
          io.puts sort_args
        }
        io.close_write
        io.each_line{|l|
          puts l  if $debug
        }
      end
    else
      sorter = @sorter.shellescape
      commands = files.map{|one_file, sort_args| "#{sorter} #{sort_args}" }
      if @num_sorters > 1
        commands.each_slice(@num_sorters) do |cmd|
          cmd = cmd.map{|c| "{ #{c} & }"}  if @could_fork
          cmd = cmd.join(' ; ')
          cmd += ' ; wait '  if @could_fork
          puts cmd  if $debug
          system cmd
        end
      else
        commands.each do |cmd|
          puts cmd  if $debug
          system cmd
        end
      end
    end
    files.each{|one_file, sort_args| one_file.write_finish}
  end
end

class SplitPgDump::Rule
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
      if field = s.scan(/\$[^\[%!]+/)
        field = field[1..-1]
        part = [field]
        while !s.eos?
          if range = s.scan(/\[[+-]?\d+\.\.\.?[+-]?\d+\]/)
            part << eval(range[1...-1])
          elsif mod = s.scan(/%\d+/)
            mod = mod[1..-1]
            format = "%0#{mod.size}d"
            modi = mod.to_i
            part << [format, modi]
          else
            break
          end
        end
        parts << part
        next if s.scan(/!/)
      elsif sep = s.scan(/[^$\s#\\]+/)
        parts << sep
        next
      end
      raise ParseError, "Wrong format of split expr #{split_expr} (rest: '#{s.rest}')"
    end
    @split_parts = parts
  end

  def parse_sort_keys(sort_keys)
    @sort_keys = (sort_keys || '').scan(/([^\s:]+)(?::([MbdfghinRrV]+))?/).map do |key, flags|
      {:field => key, :flags => flags}
    end
  end
end

class SplitPgDump::Table
  class NoColumn < StandardError; end
  ONE_FILE_CACHE_SIZE = 3 * 128 * 1024
  TOTAL_CACHE_SIZE = 4 * 128 * 1024

  class OneFile
    attr_reader :file_name, :cache_size
    def initialize(dir, name)
      @file_name = File.join(dir, name)
      @cache_lines = []
      @cache_size = 0
      dir = File.dirname(@file_name)
      unless File.directory?(dir)
        FileUtils.mkdir_p(dir)
      end
    end

    def add_line(line)
      @cache_lines << line
      @cache_size += line.size
    end

    def flush(&block)
      if @cache_size > 0
        @cache_size = 0
        content = @cache_lines.join
        File.open(@file_name, 'a'){|f| f.write(content)}
        @cache_lines.clear
      end
    end

    def write_finish
      File.open(@file_name, 'a') do |f|
        f.puts('\\.')
      end
    end

    def sort_args(sort_line = [])
      args = []
      if sort_line && !sort_line.empty?
        args.concat sort_line
      else
        args << '-n'
      end
      args.push '-o', @file_name, @file_name
      args
    end
  end

  module ComputeName
    def compute_name(split_rule, values)
      result = ''
      split_rule.each do |rule|
        case rule
        when String
          result << rule
        when Array
          field = values[rule[0]]
          rule[1].each do |action|
            case action
            when Range
              field = field[action]
            when Array # take modulo
              v = field.to_i
              field = action[0] % (v - v % action[1])
            end
          end
          result << field
        end
      end
      result
    end

    def native_compute_name?
      false
    end
  end
  if defined?(SplitPgDump::NativeComputeName)
    include SplitPgDump::NativeComputeName
  else
    include ComputeName
  end

  attr_reader :table, :columns, :files, :sort_line, :sort_args
  def initialize(dir, schema, name, columns, rule)
    @dir = dir
    @table = name
    @schema = schema
    @columns = columns.map{|c| c.sub(/^"(.+)"$/, '\\1')}
    @file_name = "#{table_schema}.dat"
    apply_rule rule
    @files = {}
    @files_to_flush = {}
    @total_cache_size = 0
  end

  def _mod(s, format, mod)
    format % (s.to_i / mod * mod)
  end

  def apply_rule(rule)
    if rule
      unless rule.split_parts.empty?
        if native_compute_name?
          @split_rule = rule.split_parts.map do |part|
            case part
            when Array # field manipulations
              unless i = @columns.index(part[0])
                raise NoColumn, "Table #{@schema}.#{@table} has no column #{part[0]} for use in split"
              end
              [i, part[1..-1]]
            else
              part
            end
          end
        else
          split_string = ''
          split_rule = []
          rule.split_parts.map do |part|
            case part
            when Array #field manipulation
              unless i = @columns.index(part[0])
                raise NoColumn, "Table #{@schema}.#{@table} has no column #{part[0]} for use in split"
              end
              field = "values[#{i}]"
              part[1..-1].each do |action|
                ssize = split_rule.size
                case action
                when Range
                  field << "[split_rule[#{ssize}]]"
                  split_rule << action
                when Array # take module
                  field = "_mod(#{field}, split_rule[#{ssize}], split_rule[#{ssize+1}])"
                  split_rule.concat action
                end
              end
              split_string << "\#{#{field}}"
            when String
              split_string << part
            end
          end
          @split_rule = split_rule
          eval <<-"EOF"
            def self.compute_name(split_rule, values)
              %{#{split_string}}
            end
          EOF
        end
        @file_name = {}
      end

      @sort_args = rule.sort_keys.map do |key|
        i = @columns.find_index(key[:field])
        raise NoColumn, "Table #{@schema}.#{@table} has no column #{key[:field]} for use in sort"  unless i
        i += 1
        "--key=#{i},#{i}#{key[:flags]}"
      end
    else
      @sort_args = []
    end
  end

  def table_schema
    @schema == 'public' ? @table : "#@schema/#@table"
  end

  def file_name(line)
    values = line.split("\t")
    values.last.chomp!
    name = compute_name(@split_rule, values)
    @file_name[name] ||= begin
      name_strip = name.gsub(/\.\.|\s|\?|\*|'|"/, '_')
      "#{table_schema}/#{name_strip}.dat"
    end
  end

  def add_line(line)
    fname = @split_rule ? file_name(line) : @file_name
    one_file = @files[fname] ||= OneFile.new(@dir, fname)

    @files_to_flush[one_file] = true  if one_file.cache_size == 0

    one_file.add_line(line)
    @total_cache_size += line.size
    if one_file.cache_size > ONE_FILE_CACHE_SIZE
      @total_cache_size -= one_file.cache_size
      one_file.flush
    end
    flush_all if @total_cache_size > TOTAL_CACHE_SIZE
  end

  def flush_all
    @files_to_flush.each{|one_file, _| one_file.flush }
    @files_to_flush.clear
    @total_cache_size = 0
  end

  def copy_lines
    if block_given?
      @files.map{|n, one_file| one_file.file_name}.sort.each do |file_name|
        yield "\\copy #{@table} (#{@columns.join(', ')}) from #{file_name}"
      end
    else
      to_enum(:copy_lines)
    end
  end
end
