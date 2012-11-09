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
      :aws_secret_access_key => cloud[:secret_access_key]})

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
      existing = DB[:running_instances].where(:instance_id => i.id)
      if existing.count > 0
        existing.update(:last_seen => now)
      else
        DB[:running_instances].insert(
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
