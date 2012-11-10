#!/usr/bin/env ruby

require 'sequel'
require 'amazon-pricing'

DB = Sequel.connect(ENV['DATABASE_URL'])

price_list = AwsPricing::PriceList.new

regionmap = {
  "us-east" => "us-east-1",
  "apac-tokyo" => "ap-northeast-1",
  "sa-east-1" => "sa-east-1",
  "apac-sin" => "ap-southeast-1",
  "us-west-2" => "us-west-2",
  "us-west" => "us-west-1",
  "eu-ireland" => "eu-west-1"
}

for region in price_list.regions
  for instance_type in region.ec2_on_demand_instance_types
    DB[:instance_ondemand_pricing].insert(
      :region => regionmap[region.name], :instance_type => instance_type.api_name,
      :price => instance_type.linux_price_per_hour * 1000
    )
  end
end

exit


typemap = 
  {"clusterGPUI"=>{"xxxxl"=>"cg1.4xlarge"},
 "hiMemODI"=>{"xxl"=>"m2.2xlarge", "xl"=>"m2.xlarge", "xxxxl"=>"m2.4xlarge"},
 "stdODI"=> {"sm"=>"m1.small", "med"=>"m1.medium", "lg"=>"m1.large", "xl"=>"m1.xlarge"},
 "hiCPUODI"=>{"med"=>"c1.medium", "xl"=>"c1.xlarge"},
 "uODI"=>{"u"=>"t1.micro"},
 "clusterComputeI"=>{"xxxxl"=>"cc1.4xlarge", "xxxxxxxxl"=>"cc2.8xlarge"},
 "hiIoODI"=>{"xxxx1"=>"hi1.4xlarge"}}

regionmap = {
  "us-east" => "us-east-1",
  "apac-tokyo" => "ap-northeast-1",
  "sa-east-1" => "sa-east-1",
  "apac-sin" => "ap-southeast-1",
  "us-west-2" => "us-west-2",
  "us-west" => "us-west-1",
  "eu-ireland" => "eu-west-1"
}

response = RestClient.get 'http://aws.amazon.com/ec2/pricing/pricing-on-demand-instances.json'
input = JSON.load(response)

if input['vers'].to_f != 0.01
  raise "Unrecognized version: #{input['vers']}"
end

for region in input['config']['regions']
  region_name = region['region']
  for instance_type in region['instanceTypes']
    instance_type_name = instance_type['type']
    for size in instance_type['sizes']
      size_name = size['size']
      puts region_name, instance_type_name, size_name
      puts regionmap[region_name], typemap[instance_type_name][size_name]
    end
  end
end
