require 'fog'
require 'koseki/autoload'
require 'csv'

module Koseki
  class Cloud < Sequel::Model

    def refresh_all
      puts "cloud=#{name} at=start"
      refresh_programmatic_billing
      for region in regions
        region.refresh_all
      end
      puts "cloud=#{name} at=finish"
    end

    def refresh_programmatic_billing
      return unless programmatic_billing_bucket

      puts "cloud=#{name} fn=refresh_programmatic_billing at=start"

      storage = Fog::Storage.new({:provider => 'AWS',
        :aws_access_key_id => access_key_id,
        :aws_secret_access_key => secret_access_key})

      bucket = storage.directories.get(programmatic_billing_bucket)

      hooks = {
        /#{account_number}-aws-billing-csv-\d\d\d\d-\d\d.csv/ => method(:import_bill),
        /#{account_number}-aws-cost-allocation-\d\d\d\d-\d\d.csv/ => nil,
        /#{account_number}-aws-billing-detailed-line-items-\d\d\d\d-\d\d.csv.zip/ => nil
      }

      for object in bucket.files
        hooks.each do |pattern, method|
          if method and pattern.match(object.key)
            method.call object
          end
        end
      end

      puts "cloud=#{name} fn=refresh_programmatic_billing at=finish"
    end

    def import_bill(object)
      puts "cloud=#{name} fn=import_bill at=start object=#{object.key}"
      already_existed = true
      bill = AWSBill.find_or_create(:cloud_id => id, :name => object.key) do |bill|
        bill.cloud_id = id
        bill.name = object.key
        bill.last_modified = object.last_modified
        already_existed = false
      end

      fresh = (already_existed and bill.last_modified == object.last_modified)

      if fresh
        puts "cloud=#{name} fn=import_bill at=fresh last_modified_db=#{bill.last_modified} last_modified_object=#{object.last_modified} fresh=#{fresh}"
        return
      end

      old_records = Koseki::AWSBillLineItem.where(:aws_bill_id => bill.id)
      old_record_count = old_records.count
      old_records.delete

      accounts = Koseki::Cloud.all.reduce({}) {|h,c| h[c.account_number] = c; h}
      unknown_accounts = {}

      field_names = []
      line_number = 0
      CSV.parse(object.body) do |row|
        line_number += 1
        if field_names.empty? # first row is headings
          field_names = row
          next
        end

        fields = Hash[field_names.zip(row)]
        account_number = fields['LinkedAccountId']
        account_name = fields['LinkedAccountName']

        if account_number and not accounts.has_key? account_number
          puts "cloud=#{name} fn=import_bill notice=unknown_account account_name=#{account_name} account_number=#{account_number}"
          accounts[account_number] = nil
        end
        
        line = Koseki::AWSBillLineItem.create do |line|
          line.aws_bill_id = bill.id
          line.last_modified = object.last_modified
          cloud = accounts[account_number]
          line.cloud_id = cloud ? cloud.id : nil
          line.line_number = line_number

          fields.each do |key, value|
            column_name = key.gsub(/::/, '/').
              gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2').
              gsub(/([a-z\d])([A-Z])/,'\1_\2').
              tr("-", "_").
              downcase
            line.send((column_name+'=').to_sym, value)
          end
        end
      end
 
      bill.update(:last_modified => object.last_modified)
        
      puts "cloud=#{name} fn=import_bill at=finish lines=#{line_number} old_records=#{old_record_count}"
    end

    class Region
      attr_reader :name

      def initialize(cloud, region=nil)
        @cloud = cloud
        @compute = Fog::Compute.new({:provider => 'AWS',
          :region => region,
          :aws_access_key_id => cloud.access_key_id,
          :aws_secret_access_key => cloud.secret_access_key,
          :version => '2012-10-01'
        })
        @name = @compute.region
      end

      def all
        # we don't have permission to do describe-regions on every account,
        # and we don't need to because the regions are the same everywhere
        @@region_names ||= @compute.describe_regions.body["regionInfo"]
        @@region_names.map do |region|
          Region.new(@cloud, region["regionName"])
        end
      end

      def refresh_all
        puts "cloud=#{@cloud.name} region=#{name} at=start"
        refresh_availability_zones
        refresh_reserved_instances
        refresh_instances
        refresh_volumes
        puts "cloud=#{@cloud.name} region=#{name} at=finish"
      end

      def refresh_availability_zones
        # http://alestic.com/2009/07/ec2-availability-zones
        #
        puts "cloud=#{@cloud.name} region=#{name} fn=refresh_availability_zones at=start"
    
        for availability_zone in @compute.describe_availability_zones.body["availabilityZoneInfo"]
          logical_az = availability_zone["zoneName"]

          begin
            Koseki::AvailabilityZoneMapping.find_or_create(
              :cloud_id => @cloud.id,
              :logical_az => logical_az
            ) do |azm|
              # find its key via reserved instance offerings
              offerings = @compute.describe_reserved_instances_offerings({
                'instance-type' => 'm1.small',
                'product-description' => 'Linux/UNIX',
                'duration' => 31536000,
                'availability-zone' => logical_az
              }).body["reservedInstancesOfferingsSet"].select {|r| r["offeringType"] == "Medium Utilization"}

              if offerings.length == 0
                raise "no RIs available"
              elsif offerings.length > 1
                # Probably AWS has added new criteria and we need a tighter filter above
                raise "multiple matches"
              end

              key = offerings.first["reservedInstancesOfferingId"]

              az = Koseki::AvailabilityZone.find_or_create(:key => key) do |az|
                az.name = "#{@cloud.name}-#{logical_az}"
                az.key = key
                az.region = name
              end

              azm.cloud_id = @cloud.id
              azm.logical_az = logical_az
              azm.availability_zone_id = az.id
            end
          rescue RuntimeError => err
            puts "cloud=#{@cloud.name} region=#{name} az=#{logical_az} at=warning message=\"Can't determine AZ mapping: #{err}\""
            next
          end
        end
        puts "cloud=#{@cloud.name} region=#{name} fn=refresh_availability_zones at=finish"

      end

      def refresh_reserved_instances
        puts "cloud=#{@cloud.name} region=#{name} fn=refresh_reserved_instances at=start"
        count = 0
        new = 0
        for ri in @compute.describe_reserved_instances.body["reservedInstancesSet"]
          Reservation.find_or_create(:id => ri["reservedInstancesId"]) do |r|
            r.id = ri["reservedInstancesId"]
            r.cloud_id = @cloud.id
            r.logical_az = ri["availabilityZone"]
            r.az_id = azmap[r.logical_az]
            r.instance_type = ri["instanceType"]
            r.instance_count = ri["instanceCount"]
            r.start_time = ri["start"]
            r.duration = ri["duration"]
            r.end_time = r.start_time + r.duration
            r.offering_type = ri["offeringType"]
            r.fixed_price = ri["fixedPrice"] * 1000
            r.usage_price = ri["usagePrice"] * 1000
            new += 1
          end
          count += 1
        end

        puts "cloud=#{@cloud.name} region=#{name} fn=refresh_reserved_instances at=finish count=#{count} new=#{new}"
      end

      def refresh_instances
        puts "cloud=#{@cloud.name} region=#{name} fn=refresh_instances at=start"
        count = 0
        new = 0
        now = Time.now
        for server in @compute.servers
          i = Koseki::Instance.find_or_create(:instance_id => server.id) do |i|
            i.cloud_id = @cloud.id
            i.instance_id = server.id
            i.logical_az = server.availability_zone
            i.az_id = azmap[i.logical_az]
            i.instance_type = server.flavor_id
            i.private_ip_address = server.private_ip_address
            i.public_ip_address = server.public_ip_address
            i.created_at = server.created_at
            i.last_seen = now
            i.region = name
            i.running = server.state == 'running'
            i.tags = Sequel::Postgres::HStore.new(server.tags)
            new += 1
          end

          if i.last_seen != now
            # if we found an existing record, update it
            i.update(:last_seen => now, :running => server.state == 'running', :tags => Sequel::Postgres::HStore.new(server.tags),
                     :private_ip_address => server.private_ip_address,
                     :public_ip_address => server.public_ip_address)
          end

          count += 1
        end

        # We just refreshed all of the instances in this region, so anything we
        # didn't see in this run is gone
        expired = Koseki::Instance.where{last_seen < now}.where(:cloud_id => @cloud.id, :running => true, :region => name)
        expired.update(:running => false)

        puts "cloud=#{@cloud.name} region=#{name} fn=refresh_instances at=finish count=#{count} new=#{new} expired=#{expired.count}"
      end

      def refresh_volumes
        puts "cloud=#{@cloud.name} region=#{name} fn=refresh_volumes at=start"
        
        new = 0
        count = 0
        now = Time.now
        for volume in @compute.volumes.all
          v = Koseki::Volume.find_or_create(:volume_id => volume.id) do |v|
            v.cloud_id = @cloud.id
            v.region = name
            v.logical_az = volume.availability_zone
            v.az_id = azmap[v.logical_az]
            v.server_id = volume.server_id
            #v.instance_id = Koseki::Instance[:instance_id => v.instance_id].id
            v.created_at = volume.created_at
            v.last_seen = now
            v.size = volume.size
            v.active = volume.state != 'deleted'
            v.tags = Sequel::Postgres::HStore.new(volume.tags)
            new += 1
          end
          count += 1

          if v.last_seen != now
            # if we found an existing record, update it
            v.update(:last_seen => now, :tags => Sequel::Postgres::HStore.new(volume.tags))
          end
        end

        # We just refreshed all of the volumes in this region, so anything we
        # didn't see in this run is gone
        expired = Koseki::Volume.where{last_seen < now}.where(:cloud_id => @cloud.id, :active => true, :region => name)
        expired.update(:active => false)

        puts "cloud=#{@cloud.name} region=#{name} fn=refresh_volumes at=finish count=#{count} new=#{new} expired=#{expired.count}"
      end

      def azmap
        @@azmap ||= {}
        for azm in Koseki::AvailabilityZoneMapping.where(:cloud_id => @cloud.id)
          @@azmap[azm.logical_az] = azm.az_id
        end
        @@azmap
      end
    end

    def regions
      @regions ||= Region.new(self).all
    end

  end

end
