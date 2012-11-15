This app is used to collect EC2 usage data to help manage AWS costs.  It
records data on running instances, reserved instances and availability zone
mappings on each of the accounts it knows about.  It also keeps a historical
record of instances which have since been terminated.

There's currently no front end; it just collects data into the attached
postgres database.  There are some interesting reports available as
[dataclips](https://dataclips.heroku.com/) associated with its database.

You can also do ad hoc queries with:

```
$ heroku pg:psql -a koseki
```
