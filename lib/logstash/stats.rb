require "singleton"
require "webrick"
require "thread"
require "cabin"
require "rubygems"

class Stats

  include Singleton

  # Each element is a map of component_name to a map.
  # The value map is mapping stat to stat value.
  attr_accessor :stat_store, :http_server, :http_server_thread

  public
  def initialize()
    @stat_store = {}
    @logger = Cabin::Channel.get(LogStash)
    @lock = Mutex.new
  end

  private
  def get_component_map(component_name)
    if ! @stat_store.has_key?(component_name)
      @stat_store[component_name] = {}
    end
    component_map = @stat_store[component_name]
    return component_map
  end

  public
  def inc(component_name, stat_name, value=1.0)
    @lock.synchronize {
      component_map = get_component_map(component_name)
      
      if ! component_map.has_key?(stat_name)
        component_map[stat_name] = value
      else
        component_map[stat_name] = component_map[stat_name] + value
      end
    }
  end

  public
  def set(component_name, stat_name, value)
    @lock.synchronize {
      component_map = get_component_map(component_name)
      component_map[stat_name] = value
    }
  end

  public
  def start_stats_server(port=8246)
    http_server = WEBrick::HTTPServer.new :Port => port
    trap 'INT' do http_server.shutdown end
    @logger.info("Starting http stats server.", :port => port)
    http_server.mount_proc '/' do |req, res|
      print_stats(req, res)
    end
    http_server_thread = Thread.new {http_server.start}
  end

  private
  def print_stats(request, response)
    response.status = 200
    response["Content-Type"] = "text/plain"
    @lock.synchronize {
      response.body = LogStash::Json.dump(@stat_store)
    }
  end

  public
  def shutdown()
    http_server.shutdown
    http_server_thread.join
  end

end