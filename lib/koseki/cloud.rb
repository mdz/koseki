require 'fog'
require 'koseki/autoload'
require 'csv'
require 'locksmith/pg'

module Koseki
  class Cloud < Sequel::Model

    def refresh_all
      puts "cloud=#{name} at=start"
      update(:updated_at => Time.now)

      refresh_programmatic_billing
      for region in regions
        region.refresh_all
      end
      puts "cloud=#{name} at=finish"
    end

    def refresh_programmatic_billing
      return unless programmatic_billing_bucket

      puts "cloud=#{name} fn=refresh_programmatic_billing at=start"

      storage = Fog::Storage.new({:provider => 'AWS',
        :aws_access_key_id => access_key_id,
        :aws_secret_access_key => secret_access_key})

      bucket = storage.directories.get(programmatic_billing_bucket)

      hooks = {
        /#{account_number}-aws-billing-csv-\d\d\d\d-\d\d.csv/ => Koseki::AWSBill.method(:refresh_from_csv_in_s3),
        /#{account_number}-aws-cost-allocation-\d\d\d\d-\d\d.csv/ => Koseki::AWSBill.method(:refresh_from_csv_in_s3),
        /#{account_number}-aws-billing-detailed-line-items-\d\d\d\d-\d\d.csv.zip/ => nil
      }

      for object in bucket.files
        hooks.each do |pattern, method|
          if method and pattern.match(object.key)
            method.call self, object
          end
        end
      end

      puts "cloud=#{name} fn=refresh_programmatic_billing at=finish"
    end

    def self.register(params)
      credentials = create_credentials(params['access_key_id'],
                                       params['secret_access_key'])

      cloud = Koseki::Cloud.find_or_create(:account_number => credentials[:account_number]) do |cloud|
        cloud.name = params['name']
        cloud.access_key_id = credentials[:access_key_id]
        cloud.secret_access_key = credentials[:secret_access_key]
      end

      # update the credentials if it already existed
      cloud.access_key_id = credentials[:access_key_id]
      cloud.secret_access_key = credentials[:secret_access_key]
      cloud.save
    end

    def self.create_credentials(account_holder_access_key_id, account_holder_secret_access_key)
      # Create limit access IAM credentials using the account holder's creds

      puts "fn=create_credentials at=start account_holder_access_key_id=#{account_holder_access_key_id}"

      iam = Fog::AWS::IAM.new({
          :aws_access_key_id => account_holder_access_key_id,
          :aws_secret_access_key => account_holder_secret_access_key})

      user = iam.users.get('koseki') || iam.users.create(:id => 'koseki')
      account_number = account_number_from_arn(user.arn)

      # clear out any older keys for the koseki user (there's a limit of 2)
      if not user.access_keys.empty?
        for key in user.access_keys
          key.destroy
        end
      end

      # We don't seem to get the secret key for existing access keys, so always
      # create a new one
      key = user.access_keys.create

      policy_document = {
        "Statement" => [
          {
            "Effect" => "Allow",
            "Action" => ["ec2:describe*"],
            "Resource" => "*"
          },
          {
            "Effect" => "Allow",
            "Action" => [
              "s3:GetBucketAcl",
              "s3:GetBucketLocation",
              "s3:GetBucketLogging",
              "s3:GetBucketNotification",
              "s3:GetBucketPolicy",
              "s3:GetBucketVersioning",
              "s3:GetLifecycleConfiguration",
              "s3:ListAllMyBuckets",
              "s3:ListBucket",
              "s3:ListBucketVersions"
            ],
            "Resource" => [
              "arn:aws:s3:::*/*"
            ]
          }
        ]
      }
      policy = user.policies.get('koseki-polling') ||
               user.policies.create(:id => 'koseki-access', :document => policy_document)
      if policy.document != policy_document
        policy.document = policy_document
        policy.save
      end

      access_key_id = key.id
      secret_access_key = key.secret_access_key

      ret = {
        :access_key_id => access_key_id,
        :secret_access_key => secret_access_key,
        :account_number => account_number
      }
      puts "fn=create_credentials at=finish access_key_id=#{ret[:access_key_id]} account_number=#{ret[:account_number]}"
      return ret
    end

    def self.account_number_from_arn(arn)
      arn.split(':')[4]
    end

    def try_lock
      Locksmith::Pg.write_lock(locksmith_id)
    end

    def lock
      Locksmith::Pg.lock(locksmith_id)
    end

    def unlock
      Locksmith::Pg.release_lock(locksmith_id)
    end

    def locksmith_id
      id.to_s
    end

    class Region
      attr_reader :name

      def initialize(cloud, region=nil)
        @cloud = cloud
        @compute = Fog::Compute.new({:provider => 'AWS',
          :region => region,
          :aws_access_key_id => cloud.access_key_id,
          :aws_secret_access_key => cloud.secret_access_key,
          :version => '2012-10-01'
        })
        @name = @compute.region
      end

      def all
        # we don't have permission to do describe-regions on every account,
        # and we don't need to because the regions are the same everywhere
        @@region_names ||= @compute.describe_regions.body["regionInfo"]
        @@region_names.map do |region|
          Region.new(@cloud, region["regionName"])
        end
      end

      def refresh_all
        puts "cloud=#{@cloud.name} region=#{name} at=start"

        # This doesn't seem to be working for most of our accounts, and just
        # reports no RIs available in every AZ.  Disabled for now since we
        # don't need it to calculate RI utilization anyway.
        #discover_availability_zones
       
        refresh_reserved_instances
        refresh_instances
        refresh_volumes
        puts "cloud=#{@cloud.name} region=#{name} at=finish"
      end

      def discover_availability_zones
        # http://alestic.com/2009/07/ec2-availability-zones
        #
        puts "cloud=#{@cloud.name} region=#{name} fn=discover_availability_zones at=start"
    
        for availability_zone in @compute.describe_availability_zones.body["availabilityZoneInfo"]
          logical_az = availability_zone["zoneName"]

          begin
            Koseki::AvailabilityZoneMapping.find_or_create(
              :cloud_id => @cloud.id,
              :logical_az => logical_az
            ) do |azm|
              # find its key via reserved instance offerings
              offerings = @compute.describe_reserved_instances_offerings({
                'instance-type' => 'm1.small',
                'product-description' => 'Linux/UNIX',
                'duration' => 31536000,
                'availability-zone' => logical_az
              }).body["reservedInstancesOfferingsSet"].select {|r| r["offeringType"] == "Medium Utilization"}

              if offerings.length == 0
                raise "no RIs available"
              elsif offerings.length > 1
                # Probably AWS has added new criteria and we need a tighter filter above
                raise "multiple matches"
              end

              key = offerings.first["reservedInstancesOfferingId"]

              az = Koseki::AvailabilityZone.find_or_create(:key => key) do |az|
                az.name = "#{@cloud.name}-#{logical_az}"
                az.key = key
                az.region = name
              end

              azm.cloud_id = @cloud.id
              azm.logical_az = logical_az
              azm.availability_zone_id = az.id
            end
          rescue RuntimeError => err
            puts "cloud=#{@cloud.name} region=#{name} fn=discover_availability_zones at=cannot_map_az az=#{logical_az} message=\"Can't determine AZ mapping: #{err}\""
            next
          end
        end
        puts "cloud=#{@cloud.name} region=#{name} fn=discover_availability_zones at=finish"

      end

      def refresh_reserved_instances
        puts "cloud=#{@cloud.name} region=#{name} fn=refresh_reserved_instances at=start"
        count = 0
        new = 0
        for ri in @compute.describe_reserved_instances.body["reservedInstancesSet"]
          Reservation.find_or_create(:id => ri["reservedInstancesId"]) do |r|
            r.id = ri["reservedInstancesId"]
            r.cloud_id = @cloud.id
            r.logical_az = ri["availabilityZone"]
            r.az_id = azmap[r.logical_az]
            r.instance_type = ri["instanceType"]
            r.instance_count = ri["instanceCount"]
            r.start_time = ri["start"]
            r.duration = ri["duration"]
            r.end_time = r.start_time + r.duration
            r.offering_type = ri["offeringType"]
            r.fixed_price = ri["fixedPrice"] * 1000
            r.usage_price = ri["usagePrice"] * 1000
            new += 1
          end
          count += 1
        end

        puts "cloud=#{@cloud.name} region=#{name} fn=refresh_reserved_instances at=finish count=#{count} new=#{new}"
      end

      def refresh_instances
        puts "cloud=#{@cloud.name} region=#{name} fn=refresh_instances at=start"
        count = 0
        new = 0
        now = Time.now
        for server in @compute.servers
          i = Koseki::Instance.find_or_create(:instance_id => server.id) do |i|
            i.cloud_id = @cloud.id
            i.instance_id = server.id
            i.logical_az = server.availability_zone
            i.az_id = azmap[i.logical_az]
            i.instance_type = server.flavor_id
            i.private_ip_address = server.private_ip_address
            i.public_ip_address = server.public_ip_address
            i.created_at = server.created_at
            i.last_seen = now
            i.region = name
            i.running = server.state == 'running'
            i.tags = Sequel::Postgres::HStore.new(server.tags)
            new += 1
          end

          if i.last_seen != now
            # if we found an existing record, update it
            i.update(:last_seen => now, :running => server.state == 'running', :tags => Sequel::Postgres::HStore.new(server.tags),
                     :private_ip_address => server.private_ip_address,
                     :public_ip_address => server.public_ip_address)
          end

          count += 1
        end

        # We just refreshed all of the instances in this region, so anything we
        # didn't see in this run is gone
        expired = Koseki::Instance.where{last_seen < now}.where(:cloud_id => @cloud.id, :running => true, :region => name)
        expired.update(:running => false)

        puts "cloud=#{@cloud.name} region=#{name} fn=refresh_instances at=finish count=#{count} new=#{new} expired=#{expired.count}"
      end

      def refresh_volumes
        puts "cloud=#{@cloud.name} region=#{name} fn=refresh_volumes at=start"
        
        new = 0
        count = 0
        now = Time.now
        for volume in @compute.volumes.all
          v = Koseki::Volume.find_or_create(:volume_id => volume.id) do |v|
            v.cloud_id = @cloud.id
            v.region = name
            v.logical_az = volume.availability_zone
            v.az_id = azmap[v.logical_az]
            v.server_id = volume.server_id
            #v.instance_id = Koseki::Instance[:instance_id => v.instance_id].id
            v.created_at = volume.created_at
            v.last_seen = now
            v.size = volume.size
            v.active = volume.state != 'deleted'
            v.tags = Sequel::Postgres::HStore.new(volume.tags)
            new += 1
          end
          count += 1

          if v.last_seen != now
            # if we found an existing record, update it
            v.update(:last_seen => now, :tags => Sequel::Postgres::HStore.new(volume.tags))
          end
        end

        # We just refreshed all of the volumes in this region, so anything we
        # didn't see in this run is gone
        expired = Koseki::Volume.where{last_seen < now}.where(:cloud_id => @cloud.id, :active => true, :region => name)
        expired.update(:active => false)

        puts "cloud=#{@cloud.name} region=#{name} fn=refresh_volumes at=finish count=#{count} new=#{new} expired=#{expired.count}"
      end

      def azmap
        @@azmap ||= {}
        for azm in Koseki::AvailabilityZoneMapping.where(:cloud_id => @cloud.id)
          @@azmap[azm.logical_az] = azm.az_id
        end
        @@azmap
      end
    end

    def regions
      @regions ||= Region.new(self).all
    end

  end

end
