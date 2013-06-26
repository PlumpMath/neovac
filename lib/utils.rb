require 'net/http'
module neuron
  def current_gen
      (Time.now.to_i / (60 * 60)) %24
  end


  def split_at(atVal)
    atVal.split '.'
  end

  def filter_metrics(hash)
    hash.select do |key, value|
      key.to_s.match(/^measure/)
    end
  end

  def strip_at(at)
    atComps = split_at at
    atComps.pop
    atComps.join('.')
  end

  def check_for_neo4j(neo4j_uri)

      begin
        http = Net::HTTP.new(neo4j_uri.host, neo4j_uri.port)
        request = Net::HTTP::Get.new(neo4j_uri.request_uri)
        request.basic_auth(neo4j_uri.user, neo4j_uri.password) if (neo4j_uri.user)
        response = http.request(request)

        if response.code != '200'
          puts response
          abort "No neo"
          return false
        end
      rescue
        abort "problem connecting"
        return false
      end
      return true
  end

  def dot(left,right)
    if left == right
      return left
    end
    if left
      return left + "." + right
    else
      return right
    end
  end
end
