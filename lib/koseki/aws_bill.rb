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

        expired_records = Koseki::AWSBillLineItem.purge(bill)
        new_records = Koseki::AWSBillLineItem.import_csv(bill, object.body)

        bill.update(:last_modified => object.last_modified)
        puts "cloud=#{cloud.name} fn=refresh_from_csv_in_s3 at=finish object=#{object.key} new_records=#{new_records} expired_records=#{expired_records}"
      end
    end
  end
end
