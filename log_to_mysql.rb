require 'rubygems'
gem 'activerecord'
gem 'activerecord-import'

require 'mysql2'
require 'active_record'
require 'activerecord-import'
require 'date'

ActiveRecord::Base.establish_connection(
  adapter: 'mysql2',
  host: '172.26.184.20',
  encoding: 'utf8',
  database: 'analysis_demo',
  username: 'analysis',
  password:'analysis',
)

VALID_DOMAINS = ["tile.api.itsjp.mtc-pas.jp",
                "gis.api.itsjp.mtc-pas.jp",
                "direction.api.itsjp.mtc-pas.jp",
                "update.api.itsjp.mtc-pas.jp",
                "auth.api.itsjp.mtc-pas.jp",
                "sns.api.itsjp.mtc-pas.jp",
                "dpa.api.itsjp.mtc-pas.jp",
                "contents.api.itsjp.mtc-pas.jp",
                "web.itsjp.mtc-pas.jp",
                "traffic.api.itsjp.mtc-pas.jp",
                "search.api.itsjp.mtc-pas.jp",
                "admin.api.itsjp.mtc-pas.jp",
                "admin.itsjp.mtc-pas.jp"]

def validate_user_agent?(user_agent)
  user_agent.include?("13IprAA") || user_agent.include?("13DdnAI")
end

def ngx_time_to_mysql(log_time)
  return nil if log_time == '-'
  valid_time = log_time.split(':', 2)
  valid_time[0] = Date.parse(valid_time[0]).to_s
  valid_time.join(' ')
end

def wrap_string(str)
  "'"+str+"'"
end

def wrap_line(line)
  splited_line = line.split
  row = []

  uuid = splited_line[-2]
  ip = splited_line[0]
  url = splited_line[6]
  time = splited_line[3]
  domain = splited_line[-5]
  user_agent = splited_line[-7]
  http_code = splited_line[8]
  transfer_bytes = splited_line[9]

  return false if  ip == '-'
  return false if  time == '-'
  return false if  url == '-'
  return false if  http_code == '-'
  return false if  transfer_bytes == '-'
  return false if  uuid == '-'
  return false if  domain == '-' || !VALID_DOMAINS.include?(domain)
  return false if  user_agent == '-' || !validate_user_agent?(user_agent)

  # uuid
  row.push(uuid)
  # ip
  row.push(ip)
  # url
  row.push(url[0..255])
  # access_time
  access_time = ngx_time_to_mysql(time[1..-1])
  row.push(access_time)
  # domain
  row.push(domain)
  # user_agent
  row.push(user_agent[1..-2])
  # http_code
  row.push(http_code)
  # transfer_bytes
  row.push(transfer_bytes)
  #  data_date
  row.push(access_time.split[0])

  row.map! { |v| wrap_string(v) }
  "("+row.join(', ')+")"
end

TIME = '28/May/2014:03:34:21 +0900'

#puts ngx_time_to_mysql(TIME)

LOG = '49.98.135.84 - - [28/May/2014:03:34:05 +0900] "POST /api/traffic/3.0/itsjp/uploadfcd?version=401 HTTP/1.1" 200 25 "-" "13IprAA/1.000030" "-" traffic.api.itsjp.mtc-pas.jp traffic-fcd-service 10.20.50.15:80 D2PcCH434392 -'

rows = []
filename = 'access-traffic_uuid_token.log-20140529'
File.open(filename) do |file|
  while line = file.gets
    rows.push(wrap_line(line)) if wrap_line(line)

    if rows.count >= 1000 || file.eof?
      values = rows.join(', ')
      unless values.blank?
        sql = "INSERT INTO access_log_tb(uuid, ip, url, access_time, domain, user_agent, http_code, transfer_bytes, data_date) VALUES#{values}"
        ActiveRecord::Base.connection.execute(sql)
      else
        puts '123'
      end
    end

  end
end

#puts rows.join(', ')

