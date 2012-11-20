Sequel.migration do
  change do
    create_view(:active_reservations, "select az_id, instance_type, sum(instance_count) as instance_count
from reservations
where now() < start + (duration_seconds*'1 second'::interval)
group by az_id, instance_type;")
  end
end
