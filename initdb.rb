#!/usr/bin/env ruby

require 'yaml'
require 'sequel'

DB = Sequel.connect(ENV['DATABASE_URL'])


DB.create_table :clouds do
  primary_key :id
  String :name, :unique => true
  String :access_key_id, :null => false
  String :secret_access_key, :null => false
end

DB.create_table :availability_zone_mappings do
  foreign_key :cloud_id, :clouds
  String :logical_az, :null => false
  Integer :availability_zone_id, :null => false
  unique ([:cloud_id, :logical_az])
end

DB.create_table :availability_zones do
  primary_key :id
  String :name, :unique => true
  String :key, :unique => true
end

DB.create_table :reservations do
  String :id, :primary_key => true
  foreign_key :cloud_id, :clouds
  String :availability_zone
  String :instance_type
  Integer :instance_count
  String :offering_type
  DateTime :start
  Integer :duration_seconds
end

DB.create_table :instances do
  foreign_key :cloud_id, :clouds
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
