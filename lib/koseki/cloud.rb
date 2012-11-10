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
  end
end
