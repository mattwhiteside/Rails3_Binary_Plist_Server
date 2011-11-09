class Dwelling < ActiveRecord::Base
   include Geokit::Geocoders
   require "open-uri"

   attr_accessor :html
   attr :userbody
   before_create :geocode_address
   DECENT_CUTOFF = 9#days

   state_machine :state, :initial => :active do
      event :archive do
         transition all => :archived
      end
   end
   default_scope without_state(:archived)




   CRAIGSLIST_REGEX = /(\$)(\d{2,8})( \/* *)(\d)*(br)*( - )(.*)/
   #$2100 / 4br - RELAX IN YOUR NEW HOME!
   STUDIO_REGEX = /(\$)(\d{2,5})( )(.*)/



   scope :within_box, lambda {|top,bottom,left,right|
      where("lat < #{top} AND lat > #{bottom} AND lng > #{left} AND lng < #{right}")
   }

   scope :created_before, lambda{|datetime|
      where("foreign_database_created_at < '#{datetime.strftime('%Y-%m-%d %H:%M:%S')}'")
   }

   scope(:created_after, lambda{|datetime| where("foreign_database_created_at > '#{datetime.strftime('%Y-%m-%d %H:%M:%S')}'")})
   #validates_uniqueness_of :foreign_database_key, :scope => :foreign_database

   acts_as_mappable :default_units => :kms,
   :default_formula => :sphere,
   :distance_field_name => :distance,
   :lat_column_name => :lat,
   :lng_column_name => :lng

   validates   :bedrooms,
   :bathrooms,
   :presence => true


   validates :url, :presence => true, :uniqueness => true


   def self.crawl_craigslist_city(city_url)
      Fiber.new do
         listing_page_url = city_url
         index = 1
         loop do
            puts "starting on #{listing_page_url}"
            page_posting_ids = []
            begin
               doc = Nokogiri::HTML(open(listing_page_url),nil,"UTF-8//IGNORE")
            rescue StandardError => e
               puts "rescuing #{listing_page_url}"
               puts "Exception class = #{e.class}"
               puts "Exception message = #{e.message}"
               puts "Backtrace:\n#{e.backtrace.join("\n")}"
               next
            end
            urls = doc.xpath("//blockquote").last.xpath("//p/a/@href").map{|node| node.value}
            puts "URLS size before = #{urls.size}"
            urls -= Dwelling.select("url").where("foreign_database = 'craigslist'").where(Arel::Table.new(:dwellings)[:url].in(urls)).map{|d| d.url}
            puts "URLS size after = #{urls.size}"
            urls.each do |url|
               Fiber.yield self.create_from_craiglist_url(url)
            end
            index += 1
            listing_page_url = "#{city_url}/index#{index*100}.html"
         end
      end
   end

   def self.create_from_craiglist_url(_url)
      _url =~ /(\/)(\d+)(.html)/
      if Regexp.last_match
         _foreign_database_id = Regexp.last_match[2]
      else
         puts "no regex match on #{_url}"
         return nil
      end
      begin
         html = Nokogiri::HTML(open(_url),nil,"UTF-8//IGNORE")
      rescue StandardError => e
         puts "rescued #{_url}\n\n"
         puts "#{e.class}\n #{e.message}\n#{e.backtrace.join("\n")}"
         return nil
      end
      h2_text = html.xpath("//h2").first.text rescue nil
      match = (h2_text =~ CRAIGSLIST_REGEX)
      match ||= (h2_text =~ STUDIO_REGEX)
      if match
         _full_address_text = nil
         _brief_description = Regexp.last_match[7]
         bedrooms = Regexp.last_match[4] || 0
         price = Regexp.last_match[2]
         html.xpath('//small/a').each do |link|
            if link.content == "google map"
               map_url = link.attributes["href"].content
               loc = CGI::unescape(map_url).gsub("http://maps.google.com/?q=loc:","")
               _full_address_text = loc.strip
               break
            end
         end
         if _full_address_text
            attrs = {:brief_description => _brief_description,:full_address_text => _full_address_text}
            dwelling = self.where(attrs).order("created_at DESC").first || self.new(attrs)
            dwelling.html = html


            dwelling.attributes = {:bedrooms => bedrooms,
               :price => price,
               :foreign_database => "craigslist",
               :foreign_database_id => _foreign_database_id,
               :url => _url}

               begin
                  dwelling.balcony = !!(dwelling.brief_description =~ /balcony/i)
                  dwelling.set_bathrooms
                  dwelling.set_created_at
                  dwelling.set_location #save happens here
               rescue StandardError => e
                  puts "rescued exception in #{_url}"
                  puts "#{e.class}"
                  puts "#{e.message}"
                  puts "#{e.backtrace.join("\n")}"
                  dwelling = nil
               end
            end
         end
         return dwelling
      end


      def set_created_at
         text_nodes = @html.xpath('//body').children.select{|node| node.text?}
         date_string = text_nodes[6].content
         self.foreign_database_created_at = DateTime.strptime(date_string.strip, "Date: %Y-%m-%d, %H:%M%p %Z")
      end

      def set_bathrooms
         @userbody ||= @html.at("div#userbody").content
         if self.brief_description =~ /(\d)( )(bath)(.*)/i
            self.bathrooms = Regexp.last_match[1]
         end
         if self.bathrooms.nil?
            html =~ /(\d)( )(bath)(.*)/i
            if Regexp.last_match
               self.bathrooms = Regexp.last_match[1]
               #puts "now we have bathrooms #{bathrooms}"
            end
         end

      end

      def set_location
         if new_record? and self.save!
         elsif self.save!
            puts "found existing #{self.url}"
         else
            puts "could not save #{self.url}"
         end     
      end

      def geocode_address
         puts "we have full address text = #{self.full_address_text}"
         result = case self.url
         when /sfbay.craigslist/
            Geokit::Geocoders::YahooGeocoder.geocode(self.full_address_text)
            #Geokit::Geocoders::UsGeocoder.geocode(self.full_address_text)
         when /newyork.craigslist|losangeles/
            Geokit::Geocoders::GoogleGeocoder.geocode(self.full_address_text)
         when /denver.craigslist|portland.craigslist/
            Geokit::Geocoders::YahooGeocoder.geocode(self.full_address_text)
         # else       
         #    Geokit::Geocoders::GoogleGeocoder.geocode(self.full_address_text)
         end 
         #puts "Result of geocode: #{result}"
         if result
            puts "creating new one, #{self.url}"
            self.lat = result.lat
            self.lng = result.lng
         else
            puts "no luck geocoding, #{self.url}"
            return false
         end
      end

      def dead?
         @html ||= Nokogiri::HTML(open(self.url),nil,"UTF-8//IGNORE")
         if h2_text = (@html.xpath("//h2").first.text rescue nil)
            if h2_text =~ /This posting has been flagged for removal/ or h2_text =~ /deleted by author/ or h2_text =~ /has expired/
               return true
            end
         end
         false
      end


      def Dwelling.archive_dead_postings
         last_good_created_at = Time.now - DECENT_CUTOFF.days
         t = Arel::Table.new(:dwellings)
         start_time = Time.now
         allowed_run_time = 9#minutes
         loop do
            if (Time.now - start_time)/60 > allowed_run_time
               puts "been running for #{allowed_run_time} minutes, calling it quits"
               break 
            end
            dwelling = Dwelling.where(t[:foreign_database_created_at].gt last_good_created_at).order(t[:foreign_database_created_at]).limit(1).first
            begin
               if dwelling.dead?
                  dwelling.destroy
                  puts "found and archived dead one, #{dwelling.url}"
               else
                  last_good_created_at = dwelling.foreign_database_created_at
                  puts "#{dwelling.url} is good, #{last_good_created_at}"
               end
            rescue OpenURI::HTTPError => error
               if error.io.status[0] == "404"
                  puts "404, archiving #{dwelling.url}"
                  dwelling.destroy
               else
                  puts "wtf, #{error.io.status}"
               end
            rescue StandardError => e
               puts "rescuing #{dwelling.url}\n#{e.class}, #{e.message}\n\n#{e.backtrace.join("\n")}"
            end
         end
      end


      def Dwelling.delete_dups
         loop do
            initial_count = Dwelling.count
            self.connection.execute <<-SQL
            delete from dwellings where dwellings.id in
            (
            SELECT 	min(dup.id)
            FROM dwellings dup		
            GROUP BY dup.brief_description, dup.full_address_text
            having count(dup.id) > 1
            order by max(created_at) desc 
            )
            SQL
            post_count = Dwelling.count
            break if post_count >= initial_count
         end
      end

   end
