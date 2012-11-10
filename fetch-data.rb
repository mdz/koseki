#!/usr/bin/env ruby

require 'fog'
require 'csv'
require 'mail'
require 'sequel'

DB = Sequel.connect(ENV['DATABASE_URL'])

reservations = []
instances = {}
regions = nil

for cloud in DB[:clouds].all
  cloud_name = cloud[:name]
  puts "cloud=#{cloud_name} at=starting"

  # Connect to the default region on the first cloud and get the list of
  # available regions
  if regions == nil
    compute = Fog::Compute.new({:provider => 'AWS',
      :aws_access_key_id => cloud[:access_key_id],
      :aws_secret_access_key => cloud[:secret_access_key]})

    regions ||= compute.describe_regions.body["regionInfo"].map {|region| region["regionName"]}
  end

  for region in regions
    puts "cloud=#{cloud_name} region=#{region} at=starting"
    compute = Fog::Compute.new({:provider => 'AWS',
      :region => region,
      :aws_access_key_id => cloud[:access_key_id],
      :aws_secret_access_key => cloud[:secret_access_key]
    })

    # discover availability zones and their mappings across accounts
    # http://alestic.com/2009/07/ec2-availability-zones

    begin
      for availability_zone in compute.describe_availability_zones.body["availabilityZoneInfo"]
        logical_az = availability_zone["zoneName"]

        # check if we already have a mapping for this AZ
        next if DB[:availability_zone_mappings].where(:cloud_id => cloud[:id], :logical_az => logical_az).count > 0

        # find its key via reserved instance offerings
        offerings = compute.describe_reserved_instances_offerings({
          'instance-type' => 'm1.small',
          'product-description' => 'Linux/UNIX',
          'duration' => 31536000,
          'availability-zone' => logical_az
        }).body["reservedInstancesOfferingsSet"].select {|r| r["offeringType"] == "Medium Utilization"}

        if offerings.length == 0
          puts "cloud=#{cloud_name} region=#{region} at=error error=\"can't determine AZ mapping (no RIs available for #{logical_az}?)\""
          next
        elsif offerings.length > 1
          # Probably AWS has added new criteria and we need a tighter filter above
          puts "cloud=#{cloud_name} region=#{region} at=error error=\"can't determine AZ mapping (multiple matches for #{logical_az})\""
          next
        end

        key = offerings.first["reservedInstancesOfferingId"]

        # check if we've seen another AZ with the same key
        matching = DB[:availability_zones].where({:key => key})
        if matching.count > 0
          # we've seen it before on another cloud, so copy the physical name from there
          physical_az = matching.first[:name]
        else
          # globally new AZ
          physical_az = "unknown-#{SecureRandom.uuid}"
          DB[:availability_zones].insert(
            :name => physical_az,
            :key => key
          )
        end

        DB[:availability_zone_mappings].insert(
          :cloud_id => cloud[:id],
          :logical_az => logical_az,
          :physical_az => physical_az,
        )
      end
    rescue Fog::Compute::AWS::Error => err
      puts "cloud=#{cloud_name} region=#{region} at=error error=\"can't determine AZ mapping (no permission)\""
    end

    num_reservations = 0
    new_reservations = 0
    for ri in compute.describe_reserved_instances.body["reservedInstancesSet"]
      if DB[:reservations].where(:id => ri["reservedInstancesId"]).count == 0
        DB[:reservations].insert(
          :id => ri["reservedInstancesId"],
          :cloud_id => cloud[:id],
          :availability_zone => ri["availabilityZone"],
          :instance_type => ri["instanceType"],
          :instance_count => ri["instanceCount"],
          :start => ri["start"],
          :duration_seconds => ri["duration"],
          :offering_type => ri["offeringType"],
        )
        new_reservations += 1
      end
      num_reservations += 1
    end

    num_servers = 0
    new_servers = 0
    now = Time.now
    for i in compute.servers
      existing = DB[:instances].where(:instance_id => i.id)
      if existing.count > 0
        existing.update(:last_seen => now)
      else
        DB[:instances].insert(
          :cloud_id => cloud[:id],
          :instance_id => i.id,
          :availability_zone => i.availability_zone,
          :instance_type => i.flavor_id,
          :created_at => i.created_at,
          :last_seen => now,
        )
        new_servers += 1
      end
      num_servers += 1
    end

    puts "cloud=#{cloud_name} region=#{region} at=finished num_servers=#{num_servers} new_servers=#{new_servers} num_reservations=#{num_reservations} new_reservations=#{new_reservations}"
  end
  puts "cloud=#{cloud_name} at=finished"
end
