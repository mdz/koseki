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

  num_reserved_instances = 0
  compute.describe_reserved_instances.body["reservedInstancesSet"].map do |ri|
    next if DB[:reservations].where(:id => ri["reservedInstancesId"])
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
    num_reserved_instances += 1
  end

  num_servers = 0
  compute.servers.map do |i|
    now = Time.now
    DB[:running_instances].insert(
      :cloud_id => cloud[:id],
      :instance_id => i.id,
      :availability_zone => i.availability_zone,
      :instance_type => i.flavor_id,
      :created_at => i.created_at,
      :seen => now,
    )
    num_servers += 1
  end

  puts "cloud=#{cloud_name} at=finished num_servers=#{num_servers} num_reserved_instances=#{num_reserved_instances}"
end
