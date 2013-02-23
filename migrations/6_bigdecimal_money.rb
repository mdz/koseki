DB.extend_datasets do
  def fetch_rows(sql)
    puts sql
    puts caller
    super
  end
end

money_columns = {
  :instance_ondemand_prices => [:price],
  :instance_reserved_prices => [:price_upfront, :price_per_hour],
  :ebs_prices => [:standard_per_gb, :standard_per_million_io,
                  :preferred_per_gb,
                  :preferred_per_iops, :s3_snaps_per_gb],
  :aws_bill_line_items => [:usage_quantity, :blended_rate, :cost_before_tax,
                           :credits, :tax_amount, :total_cost],
  :reservations => [:fixed_price, :usage_price]
}

Sequel.migration do
  transaction
  up do
    DB.drop_view(:running_instance_costs)
    DB.drop_view(:invoice_costs)

    money_columns.each do |table, columns|
      alter_table table do
        for column in columns
          set_column_type column, BigDecimal
        end
      end

      next if table == :aws_bill_line_items # has always been USD

      # convert from milliUSD
      for column in columns
        DB.run("update #{table} set #{column} = #{column} / 1000.0")
      end
    end

    DB.create_view(:invoice_costs, "
with costs as (
  select linked_account_name as account_name,
    '#'||coalesce(nullif(linked_account_id,''),payer_account_id) as account_number,
    coalesce(tags->'cloud',clouds.name, 'account #'||linked_account_id) as cloud,
    coalesce(tags->'slot', coalesce(tags->'cloud', clouds.name, 'account #'||linked_account_id)||' untagged resources') as slot,
    case
      when item_description like 'Sign up charge for subscription%' then 'reserved instance purchase'
      when credits <> 0 then 'credit'
      when product_code = 'AwsCBSupport' then 'support'
      else null end
    as fixed_cost,
    invoice_id, product_name, usage_type, operation, availability_zone, item_description, product_code, usage_quantity, blended_rate, total_cost,
    case
      when usage_type like '%BoxUsage:%' then split_part(usage_type,':',2)
      when usage_type like '%BoxUsage' then 'm1.small'
      else null
    end as instance_type,
    billing_period_start_date,
    (extract(epoch from least(last_modified, billing_period_end_date) - billing_period_start_date) / extract(epoch from billing_period_end_date - billing_period_start_date))::numeric as partial_billing_period,
    to_char(billing_period_start_date, 'YYYY-MM Mon') as month,
    case
      when billing_period_end_date > now() then 'month to date'
      else 'full month'
    end::text as month_type

  from aws_bills
    inner join aws_bill_line_items on (aws_bill_id = aws_bills.id)
    left join clouds on (clouds.account_number = linked_account_id)
  where
    aws_bills.name like '%cost-allocation%' and
    record_type = 'LinkedLineItem'
),
forecast as (
  select account_name, account_number, cloud, slot, fixed_cost,
    invoice_id, product_name, usage_type, operation, availability_zone, item_description, product_code,
    case
      when fixed_cost is not null then usage_quantity
      else usage_quantity / partial_billing_period
    end as usage_quantity,
    blended_rate,
    case
      when fixed_cost is not null then total_cost
      else total_cost / partial_billing_period
    end as total_cost,
    instance_type,
    billing_period_start_date,
    partial_billing_period,
    month,
    'full month forecast'::text as month_type
  from costs
  where month_type = 'month to date'
)
select * from costs union select * from forecast;
")
  DB.create_view(:running_instance_costs, "
WITH
running AS (
  SELECT instance_type, region, logical_az, count(instances.*) AS instance_count
  FROM instances
  WHERE running = true
  GROUP BY region, logical_az, instance_type
),
reserved AS (
  SELECT logical_az, instance_type, sum(instance_count) AS instance_count
  FROM reservations
  WHERE state = 'active'
  GROUP BY logical_az, instance_type
),
usage AS (
  SELECT running.region, running.logical_az, running.instance_type, running.instance_count AS total_running, coalesce(reserved.instance_count,0) AS total_reserved,
    least(running.instance_count, coalesce(reserved.instance_count,0)) AS running_reserved,
    running.instance_count - least(running.instance_count, coalesce(reserved.instance_count,0)) AS running_ondemand
  FROM running
    LEFT JOIN reserved USING (logical_az, instance_type)
),
reserved_costs_raw AS (
  select region, instance_type, logical_az, running_reserved, instance_count AS reservations_at_this_price, usage_price,
    fixed_price,
    duration,
    greatest(0,least(instance_count, running_reserved - coalesce(sum(instance_count) OVER previous_matching_reservations,0))) AS instances
  from usage
    inner join reservations using (region, logical_az, instance_type)
  where running_reserved > 0 and reservations.state = 'active'
  window previous_matching_reservations AS (PARTITION BY region, logical_az, instance_type ORDER BY usage_price ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)

),
reserved_costs AS (
  select region, logical_az, instance_type, sum(instances) AS instances,
    usage_price AS hourly_cost_per_instance,
    fixed_price / (duration/2628000) AS monthly_ri_amort_per_instance
  from reserved_costs_raw
  group by region, logical_az, instance_type, hourly_cost_per_instance, monthly_ri_amort_per_instance
),
ondemand_costs AS (
  select region, logical_az, instance_type, sum(running_ondemand) AS instances,
    instance_ondemand_prices.price AS hourly_cost_per_instance,
    0 AS monthly_cost_per_instance
  from usage
    left join instance_ondemand_prices USING (instance_type, region)
  where running_ondemand > 0
  group by region, logical_az, instance_type, instance_ondemand_prices.price
),
merged_costs AS (
  select *, 'reserved' as pricing from reserved_costs
    UNION ALL
  select *, 'ondemand' as pricing from ondemand_costs
)
SELECT * from merged_costs
WHERE instances > 0;
")


    alter_table :aws_bills do
      set_column_allow_null :last_modified, true
    end

    # force all billing data to be reloaded
    DB[:aws_bills].update(:last_modified => nil)

  end

end
