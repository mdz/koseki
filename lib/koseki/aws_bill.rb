require 'koseki/aws_bill_line_item.rb'
require 'zip'

module Koseki
  class AWSBill < Sequel::Model
    def self.import_s3_object(cloud, object)
      puts "cloud=#{cloud.name} fn=import_s3_object object=#{object.key} at=start"
      import_file(cloud, object.key, object.body, object.last_modified)
    end

    def self.import_file(cloud, filename, contents, last_modified)
      puts "cloud=#{cloud.name} fn=import_file filename=#{filename} at=start"

      if filename.end_with? '.zip'
        temp = Tempfile.open self.class.name
        begin
          temp.write(contents)
          temp.close
          Zip::ZipFile.open(temp.path) do |zipfile|
            for entry in zipfile.entries
              import_file(cloud, entry.name, entry.get_input_stream, entry.time)
            end
          end
        ensure
          temp.close!
        end
        return
      end
      
      fields = parse_name(filename)
      if !fields
        puts "cloud=#{cloud.name} fn=import_file filename=#{filename} at=skip"
        return
      end

      db.transaction do
        already_existed = true
        bill = AWSBill.find_or_create(:cloud_id => cloud.id, :name => filename) do |bill|
          bill.cloud_id = cloud.id
          bill.name = filename
          bill.last_modified = last_modified
          parse_name(filename).each do |key, value|
            if bill.respond_to? key
              bill.send((key+'=').to_sym, value)
            end
          end
          already_existed = false
        end

        if already_existed
          fresh = (bill.last_modified == last_modified)
          puts "cloud=#{cloud.name} fn=import_file filename=#{filename} at=fresh current=#{bill.last_modified} new=#{last_modified} fresh=#{fresh}"
          return if fresh
        end

        old_records, new_records = bill.import_data(fields['format'], contents)
        bill.update(:last_modified => last_modified)
        puts "cloud=#{cloud.name} fn=import_file at=finish new_records=#{new_records} old_records=#{old_records}"
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

    def import_data(format, body)
      if format == 'csv'
        if type == 'billing-csv' or type == 'cost-allocation' or type == 'billing-detailed-line-items' or type == 'granular-line-items'
          return replace_line_items(body)
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


    def replace_line_items(body)
      old_records = purge
      new_records = Koseki::AWSBillLineItem.import_csv(self, body)
      return old_records, new_records
    end
  end
end
