Sequel.migration do
  change do
    create_table :volumes do
      primary_key :id
      String :volume_id, :null => false, :unique => true
      foreign_key :cloud_id, :clouds
      String :region, :null => false
      String :logical_az, :null => false
      foreign_key :az_id, :availability_zones
      DateTime :created_at, :null => false
      DateTime :last_seen, :null => false
      String :server_id
      Boolean :active, :null => false
      Integer :size, :null => false
      HStore :tags
    end
  end
end
