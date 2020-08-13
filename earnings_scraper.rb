require 'date'
require 'httparty'
require 'mysql2'

start_date = nil # nil = yesterday OR 'YYYY-MM-DD'
end_date   = nil # nil = today OR 'YYYY-MM-DD'
base_url = 'https://www.zacks.com/includes/classes/z2_class_calendarfunctions_data.php?calltype=eventscal&date='

##

def strip_tags(str)
	str.gsub(/<\/?[^>]*>/, "")
end

def strip_commas(str)
	str.gsub(",", "")
end

db = Mysql2::Client.new(host: 'localhost', username: 'root', password: '', database: 'skew')

existing_dates = db.query("SELECT DISTINCT(date) FROM earnings").map { |d| d['date'] }

start_date = start_date.nil? ? Date.today.prev_day : Date.parse(start_date)
end_date = end_date.nil? ? Date.today : Date.parse(end_date)
start_date.upto(end_date) do |date|
	timestamp = DateTime.strptime("#{date.to_s} 06:00:00 +00:00", '%F %T %z').strftime('%s')
	puts "DATE: #{date.strftime('%A %F')} (#{timestamp})"

	if existing_dates.include?(date)
		puts "EXISTS - Skipping" 
		next
	end
	
	url = base_url + timestamp
	puts "GET: #{url}"
	
	retries = 0
	begin
		response = HTTParty.get(url, headers: { 'User-Agent' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)' })
		data = response['data']
	rescue => error
		puts "ERROR - Retrying (#{error.message})"
		retry if (retries += 1) < 3
	end

	next unless data

	query_values = []
	data.each do |er|
		formatted = {
			ticker:               "'#{strip_tags(er[0])}'",
			date:                 "'#{date.strftime('%F')}'",
			session:              (er[3] == '--' ? 'NULL' : "'#{er[3]}'"),
			market_cap:           "'#{strip_commas(er[2]).to_i * 1000000}'",
			estimate:             (er[4] == '--' ? 'NULL' : "'#{strip_commas(strip_tags(er[4]))}'"),
			reported:             (er[5] == '--' ? 'NULL' : "'#{strip_commas(strip_tags(er[5]))}'"),
			price_change_percent: (er[8] == '--' ? 'NULL' : "'#{strip_tags(er[8]).to_f / 100}'"),
		}

		query_values << "(#{formatted.values.join(',')})"
	end

	if query_values.any?
		query = "INSERT INTO earnings (ticker, date, session, market_cap, estimated, reported, price_change_percent) VALUES #{query_values.join(',')};"
		begin
			db.query(query)
		rescue => error
			puts "ERROR - Query: #{query}"
			puts error.message
		end
	end
end