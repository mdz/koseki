Sequel.migration do
  change do
    alter_table(:reservations) do
      add_column :usage_price, Integer
      add_column :fixed_price, Integer
    end
    alter_table(:instances) do
      add_column :reservation_id, String
    end
    create_or_replace_view(:active_reservations, "select az_id, instance_type, sum(instance_count) as instance_count, fixed_price, usage_price
from reservations
where now() < start + (duration_seconds*'1 second'::interval)
group by az_id, instance_type, fixed_price, usage_price;")
  end
end
