require 'daybreak'
require 'socket'
require 'json'
require 'timeout'
require 'rufus-scheduler'
require 'rest_client'


HOSTNAME = (`hostname -f`).chomp
INTERVAL = 600


def get_deq_metrics ()
  deq_json = {}
  response = RestClient.get 'http://<sharq-server-fqdn>:<sharq-server-http-port>/metrics/'
  json_out = JSON.parse(response)
  deq_timestamp = 0
  deq_metric = 0
  json_out['dequeue_counts'].each do |key, value|
    deq_timestamp = (deq_timestamp + key.to_i)
    deq_metric = (deq_metric + value.to_i)
  end
  dequeue_timestamp = deq_timestamp/10
  dequeue_metric = deq_metric/10
  return [dequeue_timestamp, dequeue_metric]
end


def flush_deq_metrics ()
  stats_host = "<logstash-server-fqdn>"
  stats_port = "<logstash-server-port>"
  deq_time = 0
  deq_metric = 0
  begin
    timeout(15) do
      metric_out = get_deq_metrics
      deq_time = metric_out[0]
      deq_metric = metric_out[1]
      s = TCPSocket.open(stats_host, stats_port)
      deq_old_data_hash = flush_old_data
      if deq_old_data_hash.any?
          deq_old_data_hash.each do |key, value|
          s.puts "Host: #{HOSTNAME} MetricType: dequeue Time_stamp: #{key} Metrics: #{value} "  # sending plain text data to the stats server
          end
      end
      s.puts "Host: #{HOSTNAME} MetricType: dequeue Time_stamp: #{deq_time} Metrics: #{deq_metric} "      # sending plain text data to the stats server
      s.close
    end
  rescue Timeout::Error
    puts "Timed out while Connecting to Livestats Socket"
    backup_store deq_time, deq_metric
  rescue Exception => e
    puts "Got socket error: #{e}"
    backup_store deq_time, deq_metric   # Failed metric data will be sent to store on the daybreak db file
  end
end

def backup_store (time, metric_val)  # Stores the Metric data which was failed to Flush to the stats server
  time_stamp = time
  m_val = metric_val
  db = Daybreak::DB.new "/opt/sharq-deq.db"
  db["#{time_stamp}"] = m_val
  db.flush
  db.close
end

def flush_old_data ()  # Sends back a Hash of all Old failed Metric Data
  data_hash = Hash.new
  db = Daybreak::DB.new "/opt/sharq-deq.db"
  db.each do |key, value|
    data_hash["#{key}"] = "#{value}"
    db.delete("#{key}")
  end
  db.flush
  db.close
  return data_hash
end

##### Starting the Scheduler #####

scheduler = Rufus::Scheduler.new

scheduler.every "#{INTERVAL}s" do
  flush_deq_metrics
end
scheduler.join
