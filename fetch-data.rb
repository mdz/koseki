#!/usr/bin/env ruby

require 'fog'
require 'csv'
require 'mail'
require 'sequel'

DB = Sequel.connect(ENV['DATABASE_URL'])

reservations = []
instances = {}

unknown_azs = {}

DB[:clouds].all.each do |cloud|
  cloud_name = cloud[:name]
  puts cloud_name

  azmap = {}
  Hash.new DB[:availability_zones].where(:cloud_id => cloud[:id]).map {|row| azmap[row[:logical]] = row[:physical]}

  compute = Fog::Compute.new({:provider => 'AWS',
    :aws_access_key_id => cloud[:access_key_id],
    :aws_secret_access_key => cloud[:secret_access_key]})

  if cloud_name == 'production'
    compute.describe_reserved_instances.body["reservedInstancesSet"].map do |ri|
      reservations << [ri["instanceType"],
                azmap[ri["availabilityZone"]],
                ri["instanceCount"],
                ri["start"],
                ri["duration"]]
    end
  else
    compute.servers.map do |i|
      if azmap.key? i.availability_zone
        az = azmap[i.availability_zone]
      else
        key = "#{cloud_name}/#{i.availability_zone}"
        puts "Warning: Unknown AZ: #{cloud_name}/#{i.availability_zone}" unless unknown_azs.key? key
        unknown_azs[key] = true
        az = key
      end
        
      instances[az] ||= {}
      instances[az][i.flavor_id] ||= 0
      instances[az][i.flavor_id] += 1
    end
  end
end

reservations.unshift ["Instance type", "Availability zone", "Instance count", "Start", "Duration (seconds)"]
reservations_csv = reservations.map {|row| row.to_csv}.join

instances_table = [["Instance type", "Availability zone", "Instance count"]]
instances.each do |availability_zone, hash|
  hash.each do |instance_type, count|
    instances_table << [availability_zone, instance_type, count]
  end
end
instances_csv = instances_table.map {|row| row.to_csv}.join

timestamp = Time::now.to_s

email_to = ENV['EMAIL_TO']
day_of_week = ENV['EMAIL_DAY_OF_WEEK']
if email_to and (email_day_of_week == nil or email_day_of_week == Time::now.wday.to_s)
  Mail.defaults do
    delivery_method :smtp, {
      :address => 'smtp.sendgrid.net',
      :port => '587',
      :domain => 'heroku.com',
      :user_name => ENV['SENDGRID_USERNAME'],
      :password => ENV['SENDGRID_PASSWORD'],
      :authentication => :plain,
      :enable_starttls_auto => true
    }
  end

  puts "Emailing report to #{email_to}"
  mail = Mail.new do
    from     ENV['EMAIL_FROM']
    to       ENV['EMAIL_TO']
    subject  ENV['EMAIL_SUBJECT'] % timestamp
    body     "Automatically generated by %s" % ENV['APP_NAME']
    add_file :filename => 'reserved instances %s.csv' % timestamp, :content => reservations_csv
    add_file :filename => 'running instances %s.csv' % timestamp, :content => instances_csv
  end
  mail.deliver!
else
  puts reservations_csv
  puts
  puts instances_csv
end
