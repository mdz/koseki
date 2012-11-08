#!/usr/bin/env ruby

require 'yaml'
require 'sequel'

DB = Sequel.connect(ENV['DATABASE_URL'])

DB.create_table! :clouds do
  primary_key :id
  String :name
  String :access_key_id
  String :secret_access_key
end

DB.create_table! :availability_zones do
  Integer :cloud_id
  String :logical
  String :physical
end
