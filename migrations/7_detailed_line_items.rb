Sequel.migration do
  transaction
  up do
    alter_table :aws_bill_line_items do
      add_column :subscription_id, String
      add_column :pricing_plan_id, String
      add_column :blended_cost, BigDecimal
      add_column :un_blended_rate, BigDecimal
      add_column :un_blended_cost, BigDecimal
      add_column :reserved_instance, String
    end

    alter_table :aws_bills do
      add_column :type, String
    end

    DB.run "update aws_bills set type = substring(name from '............-aws-(.*)-....-...csv')"
  end
end
