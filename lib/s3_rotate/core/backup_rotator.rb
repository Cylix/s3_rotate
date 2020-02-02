# s3_rotate
require 's3_rotate/utils/file_utils'
require 's3_rotate/utils/logging'

module S3Rotate

  #
  # BackupRotator Class
  # Handles backup rotation locally and on S3
  #
  class BackupRotator

    # logger
    include Logging

    # attributes
    attr_accessor :s3_client

    #
    # Initialize a new BackupRotator instance.
    #
    # @param s3_client    S3Client instance
    #
    # @return the newly instanciated object.
    #
    def initialize(s3_client)
      @s3_client = s3_client
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
      rotate_local(local_backups_dir, max_local)
      rotate_daily(backup_name, max_daily)
      rotate_weekly(backup_name, max_weekly)
      rotate_monthly(backup_name, max_monthly)
    end

    #
    # Rotate daily files
    #
    # @param backup_name   String containing the name of the backup being rotated
    # @param max_daily     Integer specifying the maximum number of daily backups to keep
    #   - If there are less than `max_daily` daily files: do nothing
    #   - If there are more than `max_daily` daily files: delete the oldest files to leave `max_daily` files
    #
    # The rotation works as follows:
    #   - Less than 7 days datediff between the oldest daily file and the most recent weekly file: do nothing
    #   - More than 7 days datediff between the oldest daily file and the most recent weekly file: promote the oldest daily file to weekly file
    #   - In both cases, apply the `max_daily`
    #
    # @return nothing
    #
    def rotate_daily(backup_name, max_daily)
      # get backup files
      daily_backups  = @s3_client.remote_backups(backup_name, "daily").files
      weekly_backups = @s3_client.remote_backups(backup_name, "weekly").files

      # get most recent weekly file
      recent_weekly_file = weekly_backups.last ? weekly_backups.last.key : nil

      # look through daily backups to find which oness should be promoted
      daily_backups.each do |backup|
        # promote to weekly if applicable
        if should_promote_daily_to_weekly?(backup.key, recent_weekly_file)
          recent_weekly_file = promote(backup_name, backup, "weekly").key
        end
      end

      # cleanup old files
      if daily_backups.length > max_daily
        daily_backups.each_with_index do |backup, i|
          if i < daily_backups.length - max_daily
            logger.info("removing #{backup.key}")
            backup.destroy
          end
        end
      end
    end

    #
    # Rotate weekly files
    #
    # @param backup_name   String containing the name of the backup being rotated
    # @param max_weekly    Integer specifying the maximum number of weekly backups to keep
    #   - If there are less than `max_weekly` weekly files: do nothing
    #   - If there are more than `max_weekly` weekly files: delete the oldest files to leave `max_weekly` files
    #
    # The rotation works as follows:
    #   - Less than 1 month datediff between the oldest weekly file and the most recent monthly file: do nothing
    #   - More than 1 month datediff between the oldest weekly file and the most recent monthly file: promote the oldest daily file to weekly file
    #   - In both cases, apply the `max_weekly`
    #
    # @return nothing
    #
    def rotate_weekly(backup_name, max_weekly)
      # get backup files
      weekly_backups  = @s3_client.remote_backups(backup_name, "weekly").files
      monthly_backups = @s3_client.remote_backups(backup_name, "monthly").files

      # get most recent monthly file
      recent_monthly_file = monthly_backups.last ? monthly_backups.last.key : nil

      # look through weekly backups to find which oness should be promoted
      weekly_backups.each do |backup|
        # promote to monthly if applicable
        if should_promote_weekly_to_monthly?(backup.key, recent_monthly_file)
          recent_monthly_file = promote(backup_name, backup, "monthly").key
        end
      end

      # cleanup old files
      if weekly_backups.length > max_weekly
        weekly_backups.each_with_index do |backup, i|
          if i < weekly_backups.length - max_weekly
            logger.info("removing #{backup.key}")
            backup.destroy
          end
        end
      end
    end

    #
    # Rotate monthly files
    #
    # @param backup_name   String containing the name of the backup being rotated
    # @param max_monthly   Integer specifying the maximum number of month backups to keep
    #   - If there are less than `max_monthly` monthly files: do nothing
    #   - If there are more than `max_monthly` monthly files: delete the oldest files to leave `max_monthly` files
    #
    # @return nothing
    #
    def rotate_monthly(backup_name, max_monthly)
      # get backup files
      monthly_backups = @s3_client.remote_backups(backup_name, "monthly").files

      # cleanup old files
      if monthly_backups.length > max_monthly
        monthly_backups.each_with_index do |backup, i|
          if i < monthly_backups.length - max_monthly
            logger.info("removing #{backup.key}")
            backup.destroy
          end
        end
      end
    end

    #
    # Rotate local files
    #
    # @param local_backups_path   String containing the path to the directory containing the backups
    # @param max_local            Integer specifying the maximum number of local backups to keep
    #   - If there are less than `max_local` local files: do nothing
    #   - If there are more than `max_local` local files: delete the oldest files to leave `max_local` files
    #
    # @return nothing
    #
    def rotate_local(local_backups_path, max_local)
      # get backup files
      local_backups = FileUtils::files_in_directory(local_backups_path)

      # cleanup old files
      if local_backups.length > max_local
        local_backups[0..(local_backups.length - max_local - 1)].each do |backup|
          logger.info("removing #{local_backups_path}/#{backup}")
          File.delete("#{local_backups_path}/#{backup}")
        end
      end
    end

    #
    # Check whether `daily_file` should be promoted into a weekly file
    # Only promote a daily file if the most recent weekly backup is one week old
    #
    # @param daily_file     String, filename of the daily backup to be checked for promotion
    # @param weekly_file    String, filename of the most recent weekly backup
    #
    # @return Boolean, True or False, whether the file should be promoted
    #
    def should_promote_daily_to_weekly?(daily_file, weekly_file)
      # never promote if no daily file
      return false if not daily_file

      # always promote if no weekly file
      return true if not weekly_file

      # retrieve the date of each file
      begin
        date_daily_file  = FileUtils::date_from_filename(daily_file)
        date_weekly_file = FileUtils::date_from_filename(weekly_file)
      rescue
        print "Wrong date (Date.parse in should_promote_daily_to_weekly)."
        return false
      end

      # perform date comparison
      return (date_daily_file - date_weekly_file).abs >= 7
    end

    #
    # Check whether `weekly_file` should be promoted into a monthly file
    # Only promote a weekly file if the most recent monthly backup is one month old
    #
    # @param weekly_file    String, filename of the weekly backup to be checked for promotion
    # @param monthly_file   String, filename of the most recent monthly backup
    #
    # @return Boolean, True or False, whether the file should be promoted
    #
    def should_promote_weekly_to_monthly?(weekly_file, monthly_file)
      # never promote if no weekly file
      return false if not weekly_file

      # always promote if no monthly file
      return true if not monthly_file

      # retrieve the date of each file
      begin
        date_weekly_file  = FileUtils::date_from_filename(weekly_file)
        date_monthly_file = FileUtils::date_from_filename(monthly_file)
      rescue
        print "Wrong date (Date.parse in should_promote_weekly_to_monthly)."
        return false
      end

      # perform date comparison
      return date_weekly_file.prev_month >= date_monthly_file
    end

    #
    # Promote a backup into a different type of backup backup (for example, daily into weekly)
    # This operation keeps the original daily file, and creates a new weekly backup
    #
    # @param backup_name   String containing the name of the backup being updated
    # @param file          S3 File, file to be promoted
    # @param type          String representing the type the backup is being promoted into, one of "daily", "weekly" or "monthly"
    #
    # @return created S3 Bucket File
    #
    def promote(backup_name, file, type)
      @s3_client.copy(backup_name, file, type)
    end

  end

end
