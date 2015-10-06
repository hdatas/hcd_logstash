# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/outputs/ceph_helper"
require "logstash/outputs/mem_event_queue"
require "logstash/stats"
require "securerandom"
require "json"
require 'pathname'

# This output will send events to the Ceph storage system.
# Besides, it provides the buffer store, like Facebook
# Scribe (https://github.com/facebookarchive/scribe).
# If the ceph is unavailable, it saves the events to
# a secondary store, then reads them and sends them to
# the primary store when it's available.
#
# Like Scribe, the design involves two buffers. Events
# are always buffred briefly in memory, then they are
# buffered to a secondary store if the primary store is down.
#
# For more information, see https://github.com/netskin/ceph-ruby
# and https://github.com/facebookarchive/scribe
#
# #### Usage:
# This is an example of logstash config:
# [source,ruby]
# output {
#    ceph {
#      local_file_path => "/tmp/logstash"         (optional)
#      max_file_size_mb => 40                     (optional)
#      max_mem_mb => 80                           (optional)
#      max_disk_mb => 800                         (optional)
#      seconds_before_new_file => 300             (optional)
#      flush_worker_num => 5                      (optional)
#      upload_worker_num => 5                     (optional)
#      retry_interval => 5                        (optional)
#      partition_fields => []                     (optional)
#
#      bucket => "data"                           (required)
#      endpoint => ""                             (required)
#      access_key_id => ""                        (required)
#      secret_access_key => ""                    (required)
#    }
#
class LogStash::Outputs::Ceph < LogStash::Outputs::Base
  include LogStash::Outputs::CephHelper

  config_name "ceph"

  # Set the directory where logstash will store the files before sending it to ceph
  # default to the current OS temporary directory in linux /tmp/hcd_logs/local_store/
  config :local_file_path, :validate => :string, :default => File.join(Dir.tmpdir, "hcd_logs/local_store/")

  # If this setting is omitted, the full json representation of the
  # event will be written as a single line.
  config :message_format, :validate => :string, :default => ""

  # Set the size per file.
  config :max_file_size_mb, :validate => :number, :default => 40

  # Force flush data from memory to disk if 
  # 1. total in-memory data reached max_mem_mb And
  # 2. total data size on disk hasn't reached the max_disk_mb.
  # Set the total in-memory data size. 
  config :max_mem_mb, :valiadte => :number, :default => 80

  # Set the total on disk data size.
  config :max_disk_mb, :valiadte => :number, :default => 800

  # Set the number of seconds before flushing a new file.
  # Flush is always done in bucket.
  # Flush data to disk if a bucket's earliest message in the bucket is older than seconds_before_new_file.
  config :seconds_before_new_file, :validate => :number, :default => 5 * 60

  # Set the number of workers for flushing data out. 
  config :flush_worker_num, :validate => :number, :default => 5

  # Set the number of workers to upload data into ceph. 
  config :upload_worker_num, :validate => :number, :default => 5

  # Set the time in seconds for retrying the ceph store open
  config :retry_interval, :validate => :number, :default => 5

  config :column_selector, :validate => :array, :default => []

  config :column_delimiter, :validate => :string, :default => "\001"

  # Set the file suffix. 
  config :file_suffix, :validate => :string, :required => false

  config :file_format, :validate => :string, :default => "textfile"

  # Partition the data before uploading.
  config :partition_fields, :validate => :array, :default=> []

  config :bucket, :validate => :string, :required => true

  config :hive_home, :validate => :string, :required => false
  
  config :hdfs_home, :validate => :string, :required => false
  
  config :hadoop_common_home, :validate => :string, :required => false

  #######################################
  # for aws s3, use Aws Sdk for ruby v2
  def create_bucket(bktname)
    bucket = @s3.bucket(bktname)
    if ! bucket.exists?
      @logger.debug("Creating bucket", :bucket => bktname)
      @s3.create_bucket(:bucket => bktname)
      @logger.debug("Created bucket", :bucket => bktname)
    end
  end

  def aws_s3_config
    @logger.info("Registering s3 output", :bucket => @bucket, :endpoint=> @endpoint)
    @s3 = Aws::S3::Resource.new(aws_options_hash)
  end

  private
  def inc_file_upload_stat()
    @stats_store.inc("output/#{self.class.config_name}", "remote_file_uploads")
  end

  private
  def inc_receive_stat()
    @stats_store.inc("output/#{self.class.config_name}", "received_events")
  end

  private
  def inc_local_file_flush_stat(event_num)
    @stats_store.inc("output/#{self.class.config_name}", "local_disk_events", event_num)
    @stats_store.inc("output/#{self.class.config_name}", "local_written_files")
  end

  private
  def inc_corrupted_event()
    @stats_store.inc("output/#{self.class.config_name}", "corrupted_events")
  end

  private
  def get_remote_bucket_key(local_file)
    basefile_name = File.basename(local_file)
    relative_path = Pathname.new(local_file).relative_path_from(Pathname.new(@local_file_path))
    return relative_path.to_s
  end

  private
  def upload_file(local_file)
    remote_bucket_key = get_remote_bucket_key(local_file)

    @logger.debug("Ceph: ready to write file in bucket", :local_file => local_file, :remote_bucket_key => remote_bucket_key)

    begin
      # prepare for write the file
      # TODO: how about the acl?
      obj = @s3.bucket(@bucket).object(remote_bucket_key)
      obj.upload_file(local_file)
    rescue AWS::Errors::Base => error
      @logger.error("Ceph: AWS error", :error => error)
      raise LogStash::Error, "AWS Configuration Error, #{error}"
    end

    inc_file_upload_stat()

    @logger.debug("Ceph: has written remote file in bucket.", :remote_bucket_key  => remote_bucket_key)
  end

  ########################################

  private
  def add_require_jars(search_root)
    cwd = Dir.pwd
    Dir.chdir(search_root) # multi-threaded program may throw an error
    Dir.glob(File.join(search_root, "**", "*.jar")).each do |file_name|
      require "#{file_name}"
    end
    Dir.chdir(cwd)
  end

  # initialize the output
  public
  def register
    require "thread"

    @s3 = aws_s3_config

    if @hdfs_home
      add_require_jars(@hdfs_home)
    end

    if @hadoop_common_home
      add_require_jars(@hadoop_common_home)
    end

    if @hive_home
      add_require_jars(@hive_home)
      require "logstash/outputs/hadoop_rcfile"
    end

#    create_bucket(@bucket)

    @shutdown = false
    # Queue of queue of events to flush to disk.
    @to_flush_queue = Queue.new
    # Queue of files to upload.
    @file_queue = Queue.new
    # Max data size in memory.
    @max_mem_size = @max_mem_mb * 1024 * 1024
    # Max data size on disk.
    @max_disk_size = @max_disk_mb * 1024 * 1024
    # Current memory data size.
    @total_mem_size = 0
    # Current disk data size.
    @total_disk_size = 0
    
    @max_file_size = @max_file_size_mb * 1024 * 1024

    # A map from partitions to event queue
    @part_to_events = {}
    @part_to_events_lock = Mutex.new
    @part_to_events_condition = ConditionVariable.new

    @local_file_path = File.expand_path(@local_file_path)
    @stats_store = Stats.instance
    if @column_selector.empty? and @file_format != "textfile"
      @logger.error("Column selector must be specified for non text file")
      teardown(true)
    end
    add_existing_disk_files()

    # Move event queues to to_flush_queue 
    start_queue_mover()
    start_upload_workers()
    start_flush_workers()
  end

  public
  def teardown(force = false)
    @shutdown = true
    if force
      @logger.debug("Shutdown the flush workers forcefully.")
      @flush_workers.each do |worker|
        worker.stop
      end

      @logger.debug("Shutdown the queue move worker forcefully.")
      @move_queue_thread.stop

      @logger.debug("Shutdown the upload workers forcefully.")
      @upload_workers.each do |worker|
        worker.stop
      end

    else
      @logger.debug("Gracefully shutdown the upload worker.")
    end

    @flush_workers.each do |worker|
      worker.join
    end
    @move_queue_thread.join
    @upload_workers.each do |worker|
      worker.stop
    end

  end

  private
  def add_existing_disk_files()
    if !Dir.exists?(@local_file_path)
      @logger.info("Create directory", :directory => @local_file_path)
      FileUtils.mkdir_p(@local_file_path)
    else
      # Scan the folder and load all existing files.
      cwd = Dir.pwd
      Dir.chdir(@local_file_path) # multi-threaded program may throw an error
      Dir.glob(File.join(@local_file_path, "**", "*")).each do |file_name|
        @file_queue << file_name if !File.directory? file_name
        @total_disk_size += File.size(file_name)
      end
      Dir.chdir(cwd)
    end
  end

  private
  def start_queue_mover()
    @logger.info("Starting queue mover thread.")
    @move_queue_thread = Thread.new {
      while !@shutdown
        @part_to_events_lock.synchronize {
          @part_to_events.each do |part, event_queue|
            if event_queue.seconds_since_first_event > @seconds_before_new_file || event_queue.total_size >= @max_file_size
              @to_flush_queue << event_queue
              @part_to_events.delete(part)
              @logger.info("moved one queue to to_flush_queue", :part => part)
            end
          end
        }
        sleep(@seconds_before_new_file * 0.7)
      end
    }
  end

  private
  def start_flush_workers()
    @flush_workers =[]
    if @flush_worker_num == 0
      @flush_worker_num = 1
    end

    @flush_worker_num.times do
      @flush_workers << Thread.new {flush_worker()}
    end
  end

  private
  def start_upload_workers()
    @upload_workers = []
    if @upload_worker_num == 0
      @upload_worker_num = 1
    end

    @upload_worker_num.times do
      @upload_workers << Thread.new {upload_worker()}
    end
  end

  public
  def receive(event)
    return unless output?(event)

    inc_receive_stat()

    # use json format to calculate partitions and event size.
    msg = to_json_message(event)

    if @partition_fields
      begin
        partitions = get_partitions(msg)
      rescue => e
        inc_corrupted_event()
        partitions = [""]
      end
    else
      partitions = [""]
    end

    @logger.debug("received one event", :partitions => partitions)    
    # Block when the total mem size reached up limit.
    wait_for_mem

    @part_to_events_lock.synchronize {
      if ! @part_to_events.has_key?(partitions)
        @part_to_events[partitions] = MemEventQueue.new(partitions)
      end
      event_queue = @part_to_events.fetch(partitions)
      if event_queue.seconds_since_first_event > @seconds_before_new_file || event_queue.total_size >= @max_file_size
        @to_flush_queue << event_queue
        event_queue = MemEventQueue.new(partitions)
        @part_to_events[partitions] = event_queue
      end

      @logger.debug("push one event", :partition => partitions)
      event_queue.push(event, msg.size)
      @total_mem_size += msg.size
    }

  end # def event

  private
  def wait_for_mem()
    @part_to_events_lock.synchronize {
      while @total_mem_size >= @max_mem_size
          @part_to_events_condition.wait(@part_to_events_lock)
      end
    }
  end

  private
  def get_partitions(msg)
    ret = []
    json_event = JSON.parse(msg)
    @partition_fields.each do |part|
      partition = part + "=" + json_event[part]
      @logger.debug("get one partition for event.", :partition => partition)
      ret << partition
    end
    return ret
  end

  # flush the memory events to local disk
  private
  def flush_worker()
    @logger.info("Starting one flush worker thread.")
    while !@shutdown
      event_queue = @to_flush_queue.deq
      partition_dir = @local_file_path
      event_queue.partitions.each do |part|
        partition_dir = File.join(partition_dir, part)
      end

      @logger.debug("Get a queue in flush worker.", :partition_dir => partition_dir)
      if !Dir.exists?(partition_dir)
        @logger.info("Create directory", :directory => partition_dir)
        FileUtils.mkdir_p(partition_dir)
      end

      #flush data to disk.
      event_num = event_queue.event_num
      @file_queue << write_to_tempfile(event_queue, partition_dir)
      inc_local_file_flush_stat(event_num)

      @part_to_events_lock.synchronize {
        @total_mem_size -= event_queue.total_size
        @part_to_events_condition.signal
      }
    end
  end

  # upload the local temporary files to ceph
  private
  def upload_worker()
    @logger.info("Starting one upload worker thread.")
    while !@shutdown
      file_name = @file_queue.deq

      case file_name
        when LogStash::ShutdownEvent
          @logger.debug("Ceph: upload worker is shutting down gracefuly")
          break
        else
          @logger.debug("Ceph: upload worker is uploading a new file", :file => file_name)
          move_file_to_bucket(file_name)
      end
    end
  end

  # flush the local files to ceph storage
  # if success, delete the local file, otherwise, retry
  private
  def move_file_to_bucket(file_name)
    begin
      if !File.zero?(file_name)
        upload_file(file_name)
      else
        @logger.debug("skipping file as its empty.",  :file => file_name)
      end
    rescue => e
      @logger.warn("Failed to upload the chunk file to ceph. Retry after #{@retry_interval} seconds.", :exception => e)
      sleep(@retry_interval)
      retry
    end

    begin
      File.delete(file_name)
    rescue Errno::ENOENT
      # Something else deleted the file, logging but not raising the issue
      @logger.warn("Ceph: Cannot delete the file since it doesn't exist on disk", :filename => File.basename(file))
    rescue Errno::EACCES
      @logger.error("Ceph: Logstash doesnt have the permission to delete the file in the temporary directory.", :filename => File.basename(file), :local_file_path => @local_file_path)
    end
  end

  private
  def write_to_tempfile(event_q, dir)
    events = event_q.event_queue
    create_ts = event_q.begin_ts
    filename = create_temporary_file(dir, create_ts)
    @logger.debug("Ceph: put events into tempfile ", :file => filename)
    if @file_format == "textfile"
      write_text_file(events, filename)
    elsif @file_format == "rcfile"
      write_rcfile(events, filename)
    else
      @logger.error("Non supported file format: #{@file_format}!")
      teardown(true)
    end
    return filename
  end

  private
  def write_text_file(events, filename)
    fd = File.open(filename, "w")
    begin
      while ! events.empty?
        fd.puts(select_columns(events.pop(non_block = true)).join(@column_delimiter))
      end
    rescue Errno::ENOSPC
      @logger.error("Ceph: No space left in temporary directory", :local_file_path => @local_file_path)
      teardown(true)
    ensure
      fd.close
    end
  end

  private
  def write_rcfile(events, filename)
    writer = nil
    begin
      writer = HadoopRCFile.new(filename, @column_selector.size())
      while ! events.empty?
        writer.write(select_columns(events.pop(non_block = true)))
      end
    rescue Errno::ENOSPC
      @logger.error("Ceph: No space left in temporary directory", :local_file_path => @local_file_path)
      teardown(true)
    ensure
      if writer
        writer.close()
      end
    end
  end

  private
  def select_columns(event)
    json_message = to_json_message(event)
    if @column_selector.empty?
      return [json_message]
    end
    
    json_event = JSON.parse(json_message)
    col = []
    @column_selector.each do |select|
      col << json_event[select]
    end
    return col
  end

  # format the output message
  private
  def to_json_message(event)
    if @message_format.empty?
      event.to_json
    else
      event.sprintf(@message_format)
    end
  end

  private
  def create_temporary_file(dir, ts)
    filename = File.join(dir, "#{ts.strftime("%Y-%m-%dT%H.%M")}_#{SecureRandom.uuid}")
    if @file_suffix and not @file_suffix.empty?
      filename = filename + ".#{@file_suffix}"
    end

    @logger.info("Opening file", :path => filename)

    return filename
  end
end
