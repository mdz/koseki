module Koseki
  class AWSBillLineItem < Sequel::Model
    def self.import_csv(bill, data)
      puts "fn=import_csv at=start"
      start_time = Time.now

      # Usually the headings are on the first line, but at least the cost
      # allocation report has a comment there, so skip it
      header_line = ''
      while not header_line.match(/^([\w:]+,)+[\w:]+$/)
        header_line = data.readline
      end
      csv_fields = header_line.strip.split(',')

      # Create a temporary table with columns having the same names used in
      # AWS' CSV format
      temp_table = :csv_import
      temp_table_columns = csv_fields.map &:to_sym

      db.create_table temp_table, {:temp => true} do
        for column_name in temp_table_columns
          String column_name
        end 
      end

      # Import the whole raw CSV into it (fast!)
      db.copy_into temp_table, {
        :columns => temp_table_columns,
        :data => data,
        :format => :csv,
      }

      # Generate a mapping of column names to SQL expressions which will do the conversions
      column_values = Hash.new
      for column in db.schema(self.table_name)
        column_name = column[0]
        db_type = column[1][:db_type]

        if column_name == :aws_bill_id
          column_values[column_name] = bill.id
        elsif column_name == :line_number
          column_values[column_name] = 'row_number() over () - 1'
        elsif column_name == :tags
          # collapse all user tags into a single HStore column
          tag_blob = temp_table_columns.grep(/^user:/).map do |tag_column|
            tag_name = tag_column.to_s.split(':',2)[1]
            ["'#{tag_name}'", "\"#{tag_column}\""]
          end.flatten.join(',')

          column_values[column_name] = "hstore(ARRAY[#{tag_blob}]::text[])"
        else
          # Everything else maps 1:1 with the CSV, we just need to fix up the
          # names to match

          if column_name == :invoice_id
            # inconsistent capitalization rules :-(
            csv_field = 'InvoiceID'
          else
            csv_field = column_name.to_s.split('_').map(&:capitalize).join
          end

          if csv_fields.include? csv_field
            #column_values[column_name] = "nullif(\"#{csv_field}\",'')::#{db_type}"
            column_values[column_name] = "\"#{csv_field}\"::#{db_type}"
          else
            # Skip fields which are in our schema but not in the CSV (just leave them null)
          end
        end
      end
        
      # Generate the actual query
      columns = column_values.keys
      values = column_values.values_at(*columns)
      query = "INSERT INTO aws_bill_line_items (#{columns.join(',')}) SELECT #{values.join(',')} FROM #{temp_table}"
      
      db.run query
      db.drop_table(temp_table)
      return

      new_records = 0 # XXX

      elapsed = Time.now-start_time
      puts "fn=import_csv at=finish elapsed=#{elapsed.round} records_per_second=#{new_records/elapsed}"
      return new_records
    end

  end
end
