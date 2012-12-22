module Koseki
  class AWSBill < Sequel::Model
    def self.refresh_from_csv_in_s3(cloud, object)
      puts "cloud=#{cloud.name} fn=refresh_from_csv_in_s3 object=#{object.key} at=start"
      db.transaction do
        already_existed = true
        bill = AWSBill.find_or_create(:cloud_id => cloud.id, :name => object.key) do |bill|
          bill.cloud_id = cloud.id
          bill.name = object.key
          bill.last_modified = object.last_modified
          already_existed = false
        end

        if already_existed
          fresh = (bill.last_modified == object.last_modified)
          puts "cloud=#{cloud.name} fn=refresh_from_csv_in_s3 object=#{object.key} at=fresh current=#{bill.last_modified} new=#{object.last_modified} fresh=#{fresh}"
          return if fresh
        end

        expired_records = Koseki::AWSBillLineItem.where(:aws_bill_id => bill.id)
        expired_record_count = expired_records.count
        expired_records.delete
        new_records = 0

        accounts = Koseki::Cloud.all.reduce({}) {|h,c| h[c.account_number] = c; h}

        field_names = []
        line_number = 0
        CSV.parse(object.body) do |row|
          line_number += 1
          if row[0] == 'InvoiceID'
            field_names = row
            next
          elsif row.length <= 1
            # first line of cost allocation report is a comment
            next
          end

          fields = Hash[field_names.zip(row)]
          account_number = fields['LinkedAccountId']

          line = Koseki::AWSBillLineItem.create do |line|
            new_records += 1
            line.aws_bill_id = bill.id
            line.line_number = line_number
            line.tags = Sequel::Postgres::HStore.new([])

            fields.each do |key, value|
              if key.start_with? "user:"
                # store user tags in the tags column
                tag_name = key.slice(/:(.*)$/, 1)
                line.tags[tag_name] = value
              else
                # convert CSV column heading into database column name
                column_name = key.gsub(/::/, '/').
                  gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2').
                  gsub(/([a-z\d])([A-Z])/,'\1_\2').
                  tr("-", "_").
                  downcase
                line.send((column_name+'=').to_sym, value)
              end
            end
          end
        end

        bill.update(:last_modified => object.last_modified)
        puts "cloud=#{cloud.name} fn=refresh_from_csv_in_s3 at=finish object=#{object.key} new_records=#{new_records} expired_records=#{expired_record_count}"
      end
    end
  end
end
