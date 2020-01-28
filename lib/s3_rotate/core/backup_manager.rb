require 's3_rotate/core/backup_uploader'
require 's3_rotate/core/backup_rotator'

module S3Rotate

  class BackupManager

    # attributes
    attr_accessor :s3_client
    attr_accessor :uploader
    attr_accessor :rotator

    #
    # Initialize a new BackupManager instance.
    #
    # @param key        String representing the AWS ACCESS KEY ID.
    # @param secret     String representing the AWS ACCESS KEY SECRET.
    # @param bucket     String representing the name of the bucket ot use.
    # @param region     String representing the region to conect to.
    #
    # @return the newly instanciated object.
    #
    def initialize(key, secret, bucket, region)
      @s3_client = S3Client.new(key, secret, bucket, region)
      @uploader  = BackupUploader.new(@s3_client)
      @rotator   = BackupRotator.new(@s3_client)
    end

    #
    # Upload local backup files to AWS S3
    # Only uploads new backups
    # Only uploads backups as daily backups: use `rotate` to generate the weekly & monthly files
    #
    # @param backup_name        String containing the name of the backup to upload
    # @param local_backups_path String containing the path to the directory containing the backups
    # @param date_regex         Regex returning the date contained in the filename of each backup
    #
    # @return nothing
    #
    def upload(backup_name, local_backups_path, date_regex=/\d{4}-\d{2}-\d{2}/)
      @uploader.upload(backup_name, local_backups_path, date_regex)
    end

    #
    # Rotate files (local, daily, weekly, monthly) and apply maximum limits for each type
    #
    # @param backup_name        String containing the name of the backup to rotate
    # @param local_backups_path String containing the path to the directory containing the backups
    # @param max_local          Integer specifying the maximum number of local backups to keep
    # @param max_daily          Integer specifying the maximum number of daily backups to keep
    # @param max_weekly         Integer specifying the maximum number of weekly backups to keep
    # @param max_monthly        Integer specifying the maximum number of monthly backups to keep
    #
    # @return nothing
    #
    def rotate(backup_name, local_backups_dir, max_local=3, max_daily=7, max_weekly=4, max_monthly=3)
      @rotator.rotate(backup_name, local_backups_dir, max_local, max_daily, max_weekly, max_monthly)
    end

  end

end
