#!/usr/bin/env ruby

# import credentials and AZ mappings from devcloud.yml

require 'yaml'
require 'sequel'

DB = Sequel.connect(ENV['DATABASE_URL'])

DB.create_table! :clouds do
  primary_key :id
  String :name
  String :access_key_id
  String :secret_access_key
end
clouds = DB[:clouds]

DB.create_table! :availability_zones do
  Integer :cloud_id
  String :logical
  String :physical
end
availability_zones = DB[:availability_zones]

yaml = YAML.load(STDIN)

yaml.each do |cloud, params|
  next unless params.include? :ion_readonly_access_key_id and \
              params.include? :ion_readonly_secret_access_key and \
              params.include? :zone_mapping
  next unless cloud == 'shogun' or cloud == 'production'

  clouds.insert(:name => cloud,
    :access_key_id => params[:ion_readonly_access_key_id],
    :secret_access_key => params[:ion_readonly_secret_access_key]
  )

  # is there no more direct way to get the primary key ID from the previous insert?
  cloud_id = clouds.where(:name => cloud).first[:id]

  params[:zone_mapping].each do |logical, physical|
    availability_zones << {:cloud_id => cloud_id,
      :logical => logical.to_s,
      :physical => physical}
  end
end 
