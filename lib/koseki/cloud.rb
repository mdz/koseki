require 'fog'
require 'koseki/autoload'

module Koseki
  class Cloud < Sequel::Model

    def compute(region)
      @compute ||= {}
      @compute[region] ||= Fog::Compute.new({:provider => 'AWS',
        :region => region,
        :aws_access_key_id => access_key_id,
        :aws_secret_access_key => secret_access_key
      })
    end

    def discover_availability_zones(region)
      # http://alestic.com/2009/07/ec2-availability-zones
  
      for availability_zone in compute(region).describe_availability_zones.body["availabilityZoneInfo"]
        logical_az = availability_zone["zoneName"]

        begin
          Koseki::AvailabilityZoneMapping.find_or_create(
            :cloud_id => id,
            :logical_az => logical_az
          ) do |azm|
            # find its key via reserved instance offerings
            offerings = compute(region).describe_reserved_instances_offerings({
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
              az.name = "unknown-#{SecureRandom.uuid}"
              az.key = key
              az.region = region
            end

            azm.cloud_id = id
            azm.logical_az = logical_az
            azm.availability_zone_id = az.id
          end
        rescue RuntimeError => err
          puts "cloud=#{name} region=#{region} az=#{logical_az} at=error message=\"Can't determine AZ mapping: #{err}\""
          next
        end
      end

    end

    def discover_reserved_instances(region)
      puts "cloud=#{name} region=#{region} fn=discover_reserved_instances at=start"
      count = 0
      new = 0
      for ri in compute(region).describe_reserved_instances.body["reservedInstancesSet"]
        Reservation.find_or_create(:id => ri["reservedInstancesId"]) do |r|
          r.id = ri["reservedInstancesId"]
          r.cloud_id = id
          r.availability_zone = ri["availabilityZone"]
          r.instance_type = ri["instanceType"]
          r.instance_count = ri["instanceCount"]
          r.start = ri["start"]
          r.duration_seconds = ri["duration"]
          r.offering_type = ri["offeringType"]
          new += 1
        end
        count += 1
      end

      puts "cloud=#{name} region=#{region} fn=discover_reserved_instances at=finish count=#{count} new=#{new}"
    end

    def discover_instances(region)
      puts "cloud=#{name} region=#{region} fn=discover_instances at=start"
      count = 0
      new = 0
      now = Time.now
      for server in compute(region).servers
        i = Koseki::Instance.find_or_create(:instance_id => server.id) do |i|
          i.cloud_id = id
          i.instance_id = server.id
          i.availability_zone = server.availability_zone
          i.instance_type = server.flavor_id
          i.created_at = server.created_at
          i.last_seen = now
          new += 1
        end

        if i.last_seen != now
          i.update(:last_seen => now)
        end

        count += 1
      end

      puts "cloud=#{name} region=#{region} fn=discover_instances at=finish count=#{count} new=#{new}"
    end

  end

end
