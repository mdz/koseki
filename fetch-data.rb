#!/usr/bin/env ruby

require 'fog'
require 'csv'
require 'mail'
require 'sequel'

DB = Sequel.connect(ENV['DATABASE_URL'])

reservations = []
instances = {}

DB[:clouds].all.each do |cloud|
  cloud_name = cloud[:name]
  puts "cloud=#{cloud_name} at=starting"

  azmap = {}
  DB[:availability_zones].where(:cloud_id => cloud[:id]).map do |row|
    azmap[row[:logical]] = row[:physical]
  end

  compute = Fog::Compute.new({:provider => 'AWS',
    :aws_access_key_id => cloud[:access_key_id],
    :aws_secret_access_key => cloud[:secret_access_key]})

  num_reservations = 0
  new_reservations = 0
  compute.describe_reserved_instances.body["reservedInstancesSet"].map do |ri|
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
    num_reservations += 1
  end

  num_servers = 0
  new_servers = 0
  now = Time.now
  compute.servers.map do |i|
    existing = DB[:running_instances].where(:instance_id => i.id)
    if existing.count > 0
      existing.update(:seen => now)
    else
      DB[:running_instances].insert(
        :cloud_id => cloud[:id],
        :instance_id => i.id,
        :availability_zone => i.availability_zone,
        :instance_type => i.flavor_id,
        :created_at => i.created_at,
        :seen => now,
      )
      new_servers += 1
    end
    num_servers += 1
  end

  puts "cloud=#{cloud_name} at=finished num_servers=#{num_servers} new_servers=#{new_servers} num_reservations=#{num_reservations} new_reservations=#{new_reservations}"
end
