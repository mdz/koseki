module Koseki
  class AWSBillLineItem < Sequel::Model
    def self.import_csv(bill, csv)
      field_names = []
      line_number = 0
      has_tags = false
      new_records = 0

      CSV.parse(csv) do |row|
        line_number += 1
        if row[0] == 'InvoiceID'
          field_names = row
          has_tags = field_names.grep('^user:')
          next
        elsif row.length <= 1
          # first line of cost allocation report is a comment
          next
        end

        fields = Hash[field_names.zip(row)]

        line = self.create do |line|
          new_records += 1
          line.aws_bill_id = bill.id
          line.line_number = line_number
          if has_tags
            line.tags = Sequel::Postgres::HStore.new([])
          end

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

      return new_records
    end

    def self.purge(bill)
      records = self.where(:aws_bill_id => bill.id)
      record_count = expired_records.count
      records.delete
      return record_count
    end
  end
end
