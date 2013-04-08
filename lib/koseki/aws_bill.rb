require 'koseki/aws_bill_line_item.rb'
require 'zip'
require 'tmpdir'
require 'open-uri'

module Koseki
  class AWSBill < Sequel::Model
    def self.import_s3_object(cloud, object)
      last_modified = object.last_modified
      filename = object.key
      return unless should_import?(cloud, filename, last_modified)

      Dir.mktmpdir do |tmpdir|
        path = File.join(tmpdir, object.key)
        tmpfile = open(path, 'w')
        input = open(object.url(Time.now + 3600))
        IO.copy_stream(input, tmpfile)
        tmpfile.close
        input.close
        File.utime(last_modified, last_modified, path)
        import_file(cloud, path)
      end
    end

    def self.import_file(cloud, path)
      filename = File.basename(path)
      last_modified = File.stat(path).mtime
      return unless should_import?(cloud, filename, last_modified)

      if filename.end_with? '.zip'
        filename = File.basename(filename, '.zip')
        stream = IO.popen(['gunzip', '-c', path], "r", {:in => :close})
      else
        stream = open(path)
      end
      import_stream(cloud, filename, stream, last_modified)
      stream.close
    end

    def self.should_import?(cloud, filename, last_modified)
      if filename.end_with? '.zip'
        filename = File.basename(filename, '.zip')
      end

      fields = parse_name(filename)
      if not fields
        puts "cloud=#{cloud.name} fn=should_import? filename=#{filename} at=unrecognized_filename"
        return false
      end
      format = fields['format']
      type = fields['type']

      case type
      when 'billing-csv', 'cost-allocation', 'billing-detailed-line-items', 'billing-detailed-line-items-with-resources-and-tags'
        # OK
      else
        puts "cloud=#{cloud.name} fn=should_import? filename=#{filename} format=#{format} type=#{type} at=unknown_bill_type"
        return false
      end

      case format
      when 'csv'
        # OK
      else
        puts "cloud=#{cloud.name} fn=should_import? filename=#{filename} format=#{format} type=#{type} at=unknown_file_format"
        return false
      end

      bill = AWSBill.find(:cloud_id => cloud.id, :name => filename)

      current = bill ? bill.last_modified : nil
      fresh = (current == last_modified)
      puts "cloud=#{cloud.name} fn=should_import? filename=#{filename} at=finish current=#{current} new=#{last_modified} fresh=#{fresh}"
      return !fresh
    end

    def self.import_stream(cloud, filename, stream, last_modified)
      puts "cloud=#{cloud.name} fn=import_stream filename=#{filename} at=start"

      db.transaction do
        bill = AWSBill.find_or_create(:cloud_id => cloud.id, :name => filename) do |bill|
          bill.cloud_id = cloud.id
          bill.name = filename
          bill.last_modified = last_modified

          # pull in data derived from the filename
          parse_name(filename).each do |key, value|
            if bill.respond_to? key
              bill.send((key+'=').to_sym, value)
            end
          end
        end

        old_records, new_records = bill.replace_line_items(stream)
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
