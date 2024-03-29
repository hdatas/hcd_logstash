class MemEventQueue

  def initialize(partitions)
    @partitions = partitions
    @event_queue = Queue.new
    @total_size = 0
    @begin_ts_time = Time.now
    @begin_ts = @begin_ts_time.to_i
  end

  public
  def partitions
    return @partitions
  end
  
  public
  def event_queue
    return @event_queue
  end

  public
  def push(event, size)
    @event_queue << event
    @total_size += size
  end

  public
  def total_size
    return @total_size
  end

  public
  def begin_ts
    return @begin_ts_time
  end

  public
  def seconds_since_first_event()
    return Time.now.to_i - @begin_ts
  end

  public
  def event_num
    return @event_queue.length
  end
end