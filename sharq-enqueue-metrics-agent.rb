require 'daybreak'
require 'socket'
require 'json'
require 'timeout'
require 'rufus-scheduler'
require 'rest_client'


HOSTNAME = (`hostname -f`).chomp
INTERVAL = 600


def get_enq_metrics ()
  enq_json = {}
  response = RestClient.get 'http://<sharq-server-fqdn>:<sharq-server-http-port>/metrics/'
  json_out = JSON.parse(response)
  enq_timestamp = 0
  enq_metric = 0
  json_out['enqueue_counts'].each do |key, value|
    enq_timestamp = (enq_timestamp + key.to_i)
    enq_metric = (enq_metric + value.to_i)
  end
  enqueue_timestamp = enq_timestamp/10
  enqueue_metric = enq_metric/10
  return [enqueue_timestamp, enqueue_metric]
end


def flush_enq_metrics ()
  stats_host = "<logstash-server-fqdn>"
  stats_port = "<logstash-server-port>"
  enq_time = 0
  enq_metric = 0
  begin
    timeout(15) do
      metric_out = get_enq_metrics
      enq_time = metric_out[0]
      enq_metric = metric_out[1]
      s = TCPSocket.open(stats_host, stats_port)
      enq_old_data_hash = flush_old_data
      if enq_old_data_hash.any?
          enq_old_data_hash.each do |key, value|
          s.puts "Host: #{HOSTNAME} MetricType: enqueue Time_stamp: #{key} Metrics: #{value} "  # sending plain text data to the stats server
          end
      end
      s.puts "Host: #{HOSTNAME} MetricType: enqueue Time_stamp: #{enq_time} Metrics: #{enq_metric} "      # sending plain text data to the stats server
      s.close
    end
  rescue Timeout::Error
    puts "Timed out while Connecting to Livestats Socket"
    backup_store enq_time, enq_metric
  rescue Exception => e
    puts "Got socket error: #{e}"
    backup_store enq_time, enq_metric   # Failed metric data will be sent to store on the daybreak db file
  end
end

def backup_store (time, metric_val)  # Stores the Metric data which was failed to Flush to the stats server
  time_stamp = time
  m_val = metric_val
  db = Daybreak::DB.new "/opt/sharq-enq.db"
  db["#{time_stamp}"] = m_val
  db.flush
  db.close
end

def flush_old_data ()  # Sends back a Hash of all Old failed Metric Data
  data_hash = Hash.new
  db = Daybreak::DB.new "/opt/sharq-enq.db"
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
  flush_enq_metrics
end
scheduler.join
