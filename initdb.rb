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
  String :region, :null => false
end

DB.create_table :reservations do
  String :id, :primary_key => true
  foreign_key :cloud_id, :clouds
  String :availability_zone, :null => false
  String :instance_type, :null => false
  Integer :instance_count, :null => false
  String :offering_type, :null => false
  DateTime :start, :null => false
  Integer :duration_seconds, :null => false
end

DB.create_table :instances do
  foreign_key :cloud_id, :clouds
  String :instance_id, :null => false
  DateTime :last_seen, :null => false
  String :availability_zone, :null => false
  String :instance_type, :null => false
  DateTime :created_at, :null => false
end

DB.create_table :instance_ondemand_pricing do
  String :instance_type, :null => false
  String :region, :null => false
  Integer :price, :null => false
  unique ([:instance_type, :region])
end
