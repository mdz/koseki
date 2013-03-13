module Koseki
  class AWSBillLineItem < Sequel::Model
    def self.import_csv(bill, csv)
      puts "fn=import_csv at=start"
      start_time = Time.now

      field_names = []
      column_names = []
      line_number = 0
      new_records = 0
      insert_batch = []

      CSV.parse(csv) do |row|
        if row[0] == 'InvoiceID'
          field_names = row
          column_names = field_names.map { |field_name|
            field_name.gsub(/::/, '/').
                gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2').
                gsub(/([a-z\d])([A-Z])/,'\1_\2').
                tr("-", "_").
                downcase
          }
          next
        elsif row.length <= 1
          # first line of cost allocation report is a comment
          next
        end

        record = {}
        record[:aws_bill_id] = bill.id
        record['line_number'] = line_number
        row.each_with_index do |value, index|
          field_name = field_names[index]
          if field_name.start_with? "user:"
            record['tags'] ||= Sequel::Postgres::HStore.new([])
            tag_name = field_name.slice(/:(.*)$/, 1)
            record['tags'][tag_name] = value
          else
            record[column_names[index]] = value
          end
        end
        insert_batch << record

        if insert_batch.length >= 1000
          elapsed = Time.now-start_time
          self.multi_insert(insert_batch)
          new_records += insert_batch.length
          insert_batch = []

          puts "fn=import_csv at=flush new_records=#{new_records} elapsed=#{elapsed.round} records_per_second=#{new_records/elapsed.round}" unless elapsed.round == 0
        end

        line_number += 1
      end
      # flush any remaining inserts
      if !insert_batch.empty?
        self.multi_insert(insert_batch)
        new_records += insert_batch.length
      end

      elapsed = Time.now-start_time
      puts "fn=import_csv at=finish new_records=#{new_records} elapsed=#{elapsed.round} records_per_second=#{new_records/elapsed}"
      return new_records
    end

  end
end
