#!/usr/bin/env ruby

require 'yaml'
require 'sequel'

DB = Sequel.connect(ENV['DATABASE_URL'])


DB.create_table :clouds do
  primary_key :id
  String :name
  String :access_key_id
  String :secret_access_key
end

DB.create_table :availability_zone_mappings do
  Integer :cloud_id
  String :logical_az
  String :physical_az
  unique ([:cloud_id, :logical_az])
end

DB.create_table :availability_zones do
  String :name, :primary_key => true
  String :key, :unique => true
end

DB.create_table :reservations do
  String :id, :primary_key => true
  Integer :cloud_id
  String :availability_zone
  String :instance_type
  Integer :instance_count
  String :offering_type
  DateTime :start
  Integer :duration_seconds
end

DB.create_table :instances do
  Integer :cloud_id
  String :instance_id
  DateTime :last_seen
  String :availability_zone
  String :instance_type
  DateTime :created_at
end

DB.create_table :instance_ondemand_pricing do
  String :instance_type
  String :region
  Integer :price
end
