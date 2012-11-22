require 'fog'
require 'koseki/autoload'

module Koseki
  class Cloud < Sequel::Model

    class Region
      attr_reader :name

      def initialize(cloud, region=nil)
        @cloud = cloud
        @compute = Fog::Compute.new({:provider => 'AWS',
          :region => region,
          :aws_access_key_id => cloud.access_key_id,
          :aws_secret_access_key => cloud.secret_access_key
        })
        @name = @compute.region
      end

      def all
        @compute.describe_regions.body["regionInfo"].map do |region|
          Region.new(@cloud, region["regionName"])
        end
      end

      def refresh_all
        refresh_availability_zones
        refresh_reserved_instances
        refresh_instances
        refresh_volumes
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
            r.start = ri["start"]
            r.duration_seconds = ri["duration"]
            r.offering_type = ri["offeringType"]
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
            i.tags = Sequel.hstore(server.tags)
            new += 1
          end

          if i.last_seen != now
            i.update(:last_seen => now, :running => server.state == 'running', :tags => server.tags,
                     :private_ip_address => server.private_ip_address,
                     :public_ip_address => server.public_ip_address)
          end

          count += 1
        end

        # We just refreshed all of the instances in this region, so anything we
        # didn't see is gone
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
            new += 1
          end
          count += 1

          if v.last_seen != now
            v.update(:last_seen => now)
          end
        end

        # We just refreshed all of the volumes in this region, so anything we
        # didn't see is gone
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
      @@regions ||= Region.new(self).all
    end

  end

end
