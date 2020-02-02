require 's3_rotate'

# Query the environment
aws_access_key_id     = ENV["AWS_ACCESS_KEY_ID"]
aws_access_key_secret = ENV["AWS_ACCESS_KEY_SECRET"]
bucket_name           = ENV["BUCKET_NAME"]
region                = ENV["REGION"]

if not aws_access_key_id or not aws_access_key_secret or not bucket_name or not region
  puts "Misconfigured Environment: Please make sure to set the following Environment Variables"
  puts " - AWS_ACCESS_KEY_ID"
  puts " - AWS_ACCESS_KEY_SECRET"
  puts " - BUCKET_NAME"
  puts " - REGION"
  exit -1
end

# Init S3 Rotate
backup_manager = S3Rotate::BackupManager.new(aws_access_key_id, aws_access_key_secret, bucket_name, region)

# Upload backups to S3
backup_manager.upload("backup-dojo", "/data/dojo", date_regex=/\d{4}_\d{2}_\d{2}/, date_format="%Y-%m-%d")
backup_manager.upload("backup-gitlab", "/data/gitlab/app-backup", date_regex=/\d{4}_\d{2}_\d{2}/, date_format="%Y-%m-%d")
backup_manager.upload("backup-splunk", "/data/splunk/backup", date_regex=/\d{4}_\d{2}_\d{2}/, date_format="%Y-%m-%d")
backup_manager.upload("backup-taiga", "/data/taiga", date_regex=/\d{4}_\d{2}_\d{2}/, date_format="%Y-%m-%d")
backup_manager.upload("backup-trac", "/data/trac/backup", date_regex=/\d{4}_\d{2}_\d{2}/, date_format="%Y-%m-%d")

# Rotate backups
backup_manager.rotate("backup-dojo", "/data/dojo", max_local=3, max_daily=14, max_weekly=8, max_monthly=6)
backup_manager.rotate("backup-gitlab", "/data/gitlab/app-backup", max_local=3, max_daily=14, max_weekly=8, max_monthly=6)
backup_manager.rotate("backup-splunk", "/data/splunk/backup", max_local=3, max_daily=14, max_weekly=8, max_monthly=6)
backup_manager.rotate("backup-taiga", "/data/taiga", max_local=3, max_daily=14, max_weekly=8, max_monthly=6)
backup_manager.rotate("backup-trac", "/data/trac/backup", max_local=3, max_daily=14, max_weekly=8, max_monthly=6)
