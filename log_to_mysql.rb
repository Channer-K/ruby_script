#!/usr/bin/env ruby

require 'rubygems'
gem 'activerecord'

require 'mysql2'
require 'active_record'
require 'date'

ActiveRecord::Base.establish_connection(
  adapter: 'mysql2',
  host: '',
  encoding: 'utf8',
  database: 'example',
  username: '',
  password:'',
)

User_Agent_Keywords = []

VALID_DOMAINS = []


def ngx_time_to_mysql(log_time)
  valid_time = log_time.split(':', 2)
  valid_time[0] = Date.parse(valid_time[0]).to_s
  valid_time.join(' ')
end

def validate_user_agent?(user_agent)
  User_Agent_Keywords.each do |keyword|
    return true if user_agent.include?(keyword)
  end
  return false
end

def wrap_string(str)
  "'" + str + "'"
end

def wrap_line(line)
  splited_line = line.split

  if splited_line.size == 16
    return false
  elsif splited_line.size == 17
    uuid = splited_line[-1]
    domain = splited_line[-4]
    user_agent = splited_line[-6]
  elsif splited_line.size == 18
    uuid = splited_line[-2]
    domain = splited_line[-5]
    user_agent = splited_line[-7]
  end

  ip = splited_line[0]
  url = splited_line[6]
  time = splited_line[3]
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

  row = []
  row.push(uuid)                  # uuid
  row.push(ip)                    # ip
  row.push(url[0..255])           # url
  # access_time
  access_time = ngx_time_to_mysql(time[1..-1])
  row.push(access_time)
  
  row.push(domain)                # domain
  row.push(user_agent[1..-2])     # user_agent
  row.push(http_code)             # http_code
  row.push(transfer_bytes)        # transfer_bytes
  row.push(access_time.split[0])  #  data_date

  row.map! { |v| wrap_string(v) }
  "(" + row.join(', ') + ")"
end


rows = []
filename = ARGF.argv[0] if File.file?(ARGF.argv[0])

File.open(filename) do |file|
  while line = file.gets
    rows.push(wrap_line(line)) if wrap_line(line)

    if rows.count == 1000 || file.eof?
      values = rows.join(', ')
      next if values.empty?

      sql = ""
      ActiveRecord::Base.connection.execute(sql)

      rows = []
    end
  end
end

