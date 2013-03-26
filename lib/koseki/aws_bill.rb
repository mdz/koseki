require 'koseki/aws_bill_line_item.rb'
require 'zip'

module Koseki
  class AWSBill < Sequel::Model
    def self.import_s3_object(cloud, object)
      last_modified = object.last_modified
      filename = object.key
      # don't bother fetching if we already know we're up to date
      return if fresh?(cloud, filename, last_modified)

      url = object.url(Time.now + 3600)
      if object.key.end_with? '.zip'
        # Holy /bin/sh!  I could not find a way to implement streaming
        # processing in Ruby, At least url comes from fog and should not
        # contain any shell escapes

        filename = File.basename(object.key, '.zip')
        stream = IO.popen("curl -s '#{url}' | gunzip")
      else
        stream = IO.popen(['curl', '-s', url])
      end

      import_stream(cloud, filename, stream, last_modified)
    end

    def self.import_file(cloud, path)
      filename = File.basename(path)
      last_modified = File.stat(path).mtime
      return if fresh?(cloud, filename, last_modified)

      if filename.end_with? '.zip'
        filename = File.basename(filename, '.zip')
        stream = IO.popen(['gunzip', '-c', path])
      else
        stream = open(path)
      end
      import_stream(cloud, filename, stream, last_modified)
    end

    def self.fresh?(cloud, filename, last_modified)
      if filename.end_with? '.zip'
        filename = File.basename(filename, '.zip')
      end
      bill = AWSBill.find(:cloud_id => cloud.id, :name => filename)

      current = bill ? bill.last_modified : nil
      fresh = (current == last_modified)
      puts "cloud=#{cloud.name} fn=fresh? filename=#{filename} at=fresh current=#{current} new=#{last_modified} fresh=#{fresh}"
      return fresh
    end

    def self.import_stream(cloud, filename, stream, last_modified)
      puts "cloud=#{cloud.name} fn=import_stream filename=#{filename} at=start"

      fields = parse_name(filename)
      if !fields
        puts "cloud=#{cloud.name} fn=import_stream filename=#{filename} at=skip"
        return
      end

      db.transaction do
        bill = AWSBill.find_or_create(:cloud_id => cloud.id, :name => filename) do |bill|
          bill.cloud_id = cloud.id
          bill.name = filename
          bill.last_modified = last_modified
          parse_name(filename).each do |key, value|
            if bill.respond_to? key
              bill.send((key+'=').to_sym, value)
            end
          end
        end

        old_records, new_records = bill.import_data(fields['format'], stream)
        bill.update(:last_modified => last_modified)
        puts "cloud=#{cloud.name} fn=import_stream at=finish new_records=#{new_records} old_records=#{old_records}"
      end

    end

    def self.parse_name(name)
      re = /^(?<account_number>\d+)-aws-(?<type>.*)-(?<year>\d\d\d\d)-(?<month>\d\d)\.(?<format>.*)$/
      match = re.match(name)
      return nil unless match

      fields = Hash[match.names.zip(match.captures)]
      fields['month_start'] = Time.mktime(fields['year'].to_i, fields['month'].to_i)
      return fields
    end

    def import_data(format, stream)
      if format == 'csv'
        if type == 'billing-csv' or type == 'cost-allocation' or type == 'billing-detailed-line-items'
          return replace_line_items(stream)
        else
          raise "Unknown bill type: #{type}"
        end
      else
        raise "Unknown data format: #{format}"
      end
    end

    def purge
      records = Koseki::AWSBillLineItem.where(:aws_bill_id => id)
      record_count = records.count
      records.delete
      return record_count
    end


    def replace_line_items(stream)
      old_records = purge
      new_records = Koseki::AWSBillLineItem.import_csv(self, stream)
      return old_records, new_records
    end
  end
end
