require 'fog-aws'
require 'logger'

require 's3_rotate/utils/logging'

module S3Rotate

  class S3Client

    # logger
    include Logging

    # attributes
    attr_accessor :access_key
    attr_accessor :access_secret
    attr_accessor :bucket_name
    attr_accessor :region
    attr_accessor :connection

    #
    # Initialize a new S3Client instance.
    #
    # @param key        String representing the AWS ACCESS KEY ID.
    # @param secret     String representing the AWS ACCESS KEY SECRET.
    # @param bucket     String representing the name of the bucket to use.
    # @param region     String representing the region to conect to.
    #
    # @return the newly instanciated object.
    #
    def initialize(key, secret, bucket, region)
      @access_key     = key
      @access_secret  = secret
      @bucket_name    = bucket
      @region         = region
    end

    #
    # Get the S3 bucket.
    #
    # @return Fog::Storage::AWS::Directory instance.
    #
    def bucket
      @bucket ||= connection.directories.get(bucket_name)
    end

    #
    # Get the S3 connection.
    #
    # @return Fog::Storage instance.
    #
    def connection
      @connection ||= Fog::Storage.new(provider: 'AWS', aws_access_key_id: access_key, aws_secret_access_key: access_secret, region: region)
    end

    #
    # Get the list of remote backups for a specific `backup_name` and `type`
    #
    # @param backup_name   String containing the name of the backup to retrieve
    # @param type          String, one of `daily`, `weekly` of `monthly`
    #
    # @return Fog::Storage::AWS::Directory instance.
    #
    def remote_backups(backup_name, type)
      connection.directories.get(bucket_name, prefix: "#{backup_name}/#{type}")
    end

    #
    # Check if a remote backup exists
    #
    # @param backup_name   String containing the name of the backup to retrieve
    # @param backup_date   Date representing the date of the backup
    # @param type          String, one of `daily`, `weekly` of `monthly`
    # @param extension     Optional, String containing the file extension of the backup
    #
    # @return Boolean, True or False, whether the remote backup exists
    #
    def exists?(backup_name, backup_date, type, extension=nil)
      connection.directories.get(bucket_name, prefix: "#{backup_name}/#{type}/#{backup_date.to_s}#{extension}").files.any?
    end

    #
    # Upload raw data to AWS S3
    #
    # @param backup_name    String containing the name of the backup to upload
    # @param backup_date    Date representing the date of the backup
    # @param type           String representing the type of backup being uploaded, one of "daily", "weekly" or "monthly"
    # @param extension      String containing the file extension of the backup (nil if not needed)
    # @param data           String containing the data to be uploaded
    #
    # @return created S3 Bucket File
    #
    def upload(backup_name, backup_date, type, extension, data)
      logger.info("uploading #{backup_name}/#{type}/#{backup_date.to_s}#{extension}")

      # 104857600 bytes => 100 megabytes
      bucket.files.create(key: "#{backup_name}/#{type}/#{backup_date.to_s}#{extension}", body: data, multipart_chunk_size: 104857600)
    end

    #
    # Copy an existing file on AWS S3
    #
    # @param backup_name   String containing the name of the backup being updated
    # @param file          S3 File, file to be copied
    # @param type          String representing the type the backup is being copied to, one of "daily", "weekly" or "monthly"
    #
    # @return created S3 Bucket File
    #
    def copy(backup_name, file, type)
      logger.info("copying #{file.key} to #{backup_name}/#{type}/#{file.key.split('/').last}")

      # can't copy files >5GB
      # need to download them first, then re-upload them using multipart upload
      # also download them to disk to prevent exceeding memory limits
      open("s3_rotate.download.tmp", 'w+') do |f|
        bucket.files.get(file.key) do |chunk,remaining_bytes,total_bytes|
          f.write chunk
        end
      end

      # 104857600 bytes => 100 megabytes
      remote_file = bucket.files.create(key: "#{backup_name}/#{type}/#{file.key.split('/').last}", body: File.open("s3_rotate.download.tmp"), multipart_chunk_size: 104857600)

      # cleanup
      File.delete("s3_rotate.download.tmp")

      # return remote file
      remote_file
    end

  end

end
