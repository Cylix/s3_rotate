# s3_rotate
require 's3_rotate/utils/file_utils'

module S3Rotate

  #
  # BackupUploader Class
  # Handles backup uploads with the right format
  #
  class BackupUploader

    # attributes
    attr_accessor :s3_client

    #
    # Initialize a new BackupUploader instance.
    #
    # @param s3_client    S3Client instance
    #
    # @return the newly instanciated object.
    #
    def initialize(s3_client)
      @s3_client = s3_client
    end

    #
    # Upload local backup files to AWS S3
    # Only uploads new backups
    # Only uploads backups as daily backups: use `rotate` to generate the weekly & monthly files
    #
    # @param backup_name        String containing the name of the backup to upload
    # @param local_backups_path String containing the path to the directory containing the backups
    # @param date_regex         Regex returning the date contained in the filename of each backup
    # @param date_format        Format to be used by DateTime.strptime to parse the extracted date
    #
    # @return nothing
    #
    def upload(backup_name, local_backups_path, date_regex=/\d{4}-\d{2}-\d{2}/, date_format="%Y-%m-%d")
      # get backup files
      local_backups = FileUtils::files_in_directory(local_backups_path).reverse

      # upload local backups until we find one backup already uploaded
      local_backups.each do |local_backup|
        # parse the date & extension
        backup_date      = FileUtils::date_from_filename(local_backup, date_regex, date_format)
        backup_extension = FileUtils::extension_from_filename(local_backup)

        # skip invalid files
        next if not backup_date

        # stop uploading once we reach a file already uploaded
        break if @s3_client.exists?(backup_name, backup_date, "daily", extension=backup_extension)

        # upload file
        @s3_client.upload(backup_name, backup_date, "daily", backup_extension, File.open("#{local_backups_path}/#{local_backup}"))
      end
    end

  end

end
