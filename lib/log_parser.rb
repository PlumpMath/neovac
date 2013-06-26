
require 'logfmt'

def parse_logfmt(str)
  hash = {}
  result = {}
  monitor "parse_logfmt" do
    if str == ""
      return {}
    end
    parts = str.split " - "
    if parts.length != 2
      measure("parse_logfmt.invalid_log",1)
      return {}
    end

    header = parts[0]
    logs = parts[1]
    hash = parse_log_header header
    result = hash.merge Logfmt.parse logs
    measure("parse_logfmt.valid_log",1)
  end
  return result
end

def parse_log_header(header)
  hash = {}
  monitor "parse_log_header" do
    headerparts = header.split " "
    hash[:timestamp] = Time.parse(headerparts[1]).to_f
    hash[:dyno] = headerparts[4]
    hash[:ps_name] = hash[:dyno].split(".")[0] if hash[:dyno]
  end
  return hash
end


