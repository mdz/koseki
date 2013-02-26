#!/usr/bin/env ruby

require 'fog'

iam = Fog::AWS::IAM.new({
  :aws_access_key_id => ENV['AWS_ACCESS_KEY_ID'],
  :aws_secret_access_key => ENV['AWS_SECRET_ACCESS_KEY']})

user = iam.users.get('koseki') || iam.users.create(:id => 'koseki')

# We don't seem to get the secret key for existing access keys, so always
# create a new one
if not user.access_keys.empty?
  for key in user.access_keys
    key.destroy
  end
end
key = user.access_keys.create

policy_document = {
  "Statement" =>
  [
   {
     "Effect" => "Allow",
     "Action" => ["ec2:describe*"],
     "Resource" => "*"
   },
   {
     "Effect" => "Allow",
     "Action" => [
                  "s3:GetBucketAcl",
                  "s3:GetBucketLocation",
                  "s3:GetBucketLogging",
                  "s3:GetBucketNotification",
                  "s3:GetBucketPolicy",
                  "s3:GetBucketVersioning",
                  "s3:GetLifecycleConfiguration",
                  "s3:ListAllMyBuckets",
                  "s3:ListBucket",
                  "s3:ListBucketVersions"
                 ],
     "Resource" => ["arn:aws:s3:::*/*"]
   }
  ]
}
policy = user.policies.get('koseki-polling') ||
         user.policies.create(:id => 'koseki-access', :document => policy_document)
if policy.document != policy_document
  policy.document = policy_document
  policy.save
end

    
puts "User id: #{user.id}"
puts "Access key id: #{key.id}"
puts "Secret access key: #{key.secret_access_key}"
exit
