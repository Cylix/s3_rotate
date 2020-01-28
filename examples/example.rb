#!/usr/bin/env ruby

require 's3_rotate'

backup_manager = S3Rotate::BackupManager.new('aws_access_key_id', 'aws_secret_access_key', 'bucket_name', 'region')
backup_manager.upload("defect-dojo-backup", "/var/opt/defect-dojo/backups")
backup_manager.rotate("defect-dojo-backup", "/var/opt/defect-dojo/backups")
