Sequel.migration do
  change do
    alter_table(:reservations) do
      add_column :end_time, DateTime
      rename_column :start, :start_time
      rename_column :duration_seconds, :duration
    end
    DB.run("update reservations set end_time = start_time + duration*'1 second'::interval")
  end
end
