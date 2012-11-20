Sequel.migration do
  up do
    alter_table(:availability_zone_mappings) do
      rename_column :availability_zone_id, :az_id
    end

    alter_table(:reservations) do
      rename_column :availability_zone, :logical_az
      add_foreign_key :az_id, :availability_zones
    end

    alter_table(:instances) do
      rename_column :availability_zone, :logical_az
      add_foreign_key :az_id, :availability_zones
    end

    self["UPDATE reservations SET az_id = availability_zone_mappings.az_id FROM availability_zone_mappings WHERE availability_zone_mappings.cloud_id = reservations.cloud_id AND availability_zone_mappings.logical_az = reservations.logical_az"]
    self["UPDATE instances SET az_id = availability_zone_mappings.az_id FROM availability_zone_mappings WHERE availability_zone_mappings.cloud_id = instances.cloud_id AND availability_zone_mappings.logical_az = instances.logical_az"]

  end
end
