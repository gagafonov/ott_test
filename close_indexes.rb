#!/usr/bin/env ruby

require 'net/http'

poolSize = 10
fromDate = [2019, 03, 22]
toDate = [2019, 03, 29]
serviceProto = 'http://'
serviceHost = '127.0.0.1:9200'
serviceParams = {
    :getIndexes => '_cat/indices?v',
    :closeIndex => '%s/_close',
}
filter = ARGV[0]

def arrayToDig dateArray = []
    result = 0
    if dateArray.respond_to?("each")
        result = ''
        dateArray.each do |item|
            result.concat item.to_i.to_s
        end
    end

    return result.to_i
end

if ARGV.length < 1
    puts "no index specified in first launch parameter"
    exit
end

puts "working on index #{filter} from #{fromDate} to #{toDate}"

begin
    uri = URI "#{serviceProto}#{serviceHost}/#{serviceParams[:getIndexes]}"
    response = Net::HTTP.get_response uri
    indexes = response.body

    if response.is_a? Net::HTTPSuccess
        fromDate = arrayToDig fromDate
        toDate = arrayToDig toDate
        indexes = indexes.split(/\n/)

        if indexes.count > 1
            indexesToClose = Queue.new

            indexes.drop(1).each do |item| # drop headers line
                health, status, index = item.split

                if index =~ /^(.*\-)?#{filter}\-.*$/ and status == 'open' and health == 'green'
                    logDate = arrayToDig index.split(/-/).last.split(/\./)

                    if logDate > 0
                        if logDate >= fromDate and logDate <= toDate
                            indexesToClose.push index
                        end
                    else
                        p "index #{index} has no valid date"
                    end
                end
            end

            if !indexesToClose.empty? # execute queue jobs
                workers = (poolSize).times.map do
                    Thread.new do
                        begin
                            while index = indexesToClose.pop(true)
                                begin
                                    uri = URI sprintf "#{serviceProto}#{serviceHost}/#{serviceParams[:closeIndex]}", index
                                    http = Net::HTTP.new uri.host, uri.port
                                    request = Net::HTTP::Post.new "#{uri.path}?#{uri.query}"
                                    response = http.request request
                
                                    if response.is_a? Net::HTTPSuccess
                                        p "index #{index} succesfully closed"
                                    else
                                        p "index #{index} not closed: http code #{response.code}"
                                    end
                                rescue => exception
                                    p "cannot close index #{index}: #{exception} on line #{__LINE__}"
                                end
                            end
                        rescue ThreadError
                        end
                    end
                end

                workers.map &:join
            else
                p "no indexes found to close"
            end
        else
            p "no indexes found"
            exit
        end
    else
        p "get indexes failed: http code #{response.code}"
    end
rescue => exception
    p "cannot fetch indexes: #{exception} on line #{__LINE__}"
end
