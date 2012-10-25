#!/usr/bin/env ruby

require 'fog'
require 'csv'
require 'mail'

compute = Fog::Compute.new({:provider => 'AWS',
:aws_access_key_id => ENV['AWS_ACCESS_KEY_ID'],
:aws_secret_access_key => ENV['AWS_SECRET_ACCESS_KEY']})

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

reserved_instances = compute.describe_reserved_instances

table = reserved_instances.body["reservedInstancesSet"].map do |ri|
  [ri["instanceType"],
   ri["availabilityZone"],
   ri["instanceCount"],
   ri["start"],
   ri["duration"]]
end

csv = table.map {|row| row.to_csv}.join
puts csv

timestamp = Time::now.to_s

mail = Mail.new do
  from     ENV['EMAIL_FROM']
  to       ENV['EMAIL_TO']
  subject  ENV['EMAIL_SUBJECT'] % timestamp
  add_file :filename => 'reserved instances %s' % timestamp, :content => csv
end
mail.deliver!
