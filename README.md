# Koseki

This app is used to collect EC2 usage data to help manage AWS costs. It records data on running instances, reserved instances and availability zone mappings on each of the accounts it knows about. It also keeps a historical record of instances which have since been terminated.

Koseki also imports the data provided through AWS' programmatic billing interface into the database so that it can be queried easily.

There's currently no front end; it just collects data into the attached postgres database.

## Data Flow

The worker runs with: bundle exec bin/koseki poll-forever

That command executes a loop that does the following:

1. Find least recently updated cloud in the `clouds` table
2. Import AWS bill data
	* read from billing-csv, cost-allocation, billing-detailed-line-items files
	* insert data into `aws_bills` and `aws_bill_line_items` tables
2. Refresh the reserved instances for the cloud
	* Use fog and call `describe_reserved_instances`
3. Refresh the running instances for the cloud
	* Use fog and call `servers`
4. Refresh the volumes for the cloud
	* Use fog and call `volumes.all`

There is a daily task scheduled: `bundle exec bin/koseki get-prices`. This task uses a gem to retrieve AWS pricing. Pricing data is populated into `instance_ondemand_prices`, `instance_reserved_prices`, and `ebs_prices` tables.

Web process launches with `bundle exec bin/koseki web`. POST to /register-cloud will register new clouds to have their data collected.

## Schema

![schema](http://f.cl.ly/items/14423Q3C3h3H2y3P3Z0u/Koseki-ER.jpg)

### Tables

* availability_zone_mappings - for mapping resources to available zones. This functionality is disabled for the time being.
* availability_zones - for mapping resources to available zones. This functionality is disabled for the time being.
* aws_bill_line_items - AWS bill data retrieved from CSV files in the programmatic billing S3 bucket in each Amazon account
* aws_bills - AWS bill data retrieved from CSV files in the aws-programmatic-billing-drop-zone S3 bucket in the main Amazon account
* clouds - list of clouds to store data for. Clouds are added via the API.
* ebs_prices - pricing data retrieved from the AWS api
* instance_ondemand_prices - pricing data retrieved from the AWS api
* instance_reserved_prices - pricing data retrieved from the AWS api
* instances - running instances for each cloud. retrieved from fog (servers)
* reservations - reserved instances for each cloud. retieved from fog (describe_reserved_instances)
* volumes - EBS volumes for each cloud. retrieved from fog (volumes.all)
