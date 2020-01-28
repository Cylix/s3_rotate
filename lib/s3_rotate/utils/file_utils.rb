require 'date'

module S3Rotate

  module FileUtils

    #
    # Parse the date in a filename
    # Date can be any format recognize by Date.parse, or be a timestamp
    #
    # @param filename     String containing the filename to be parsed.
    # @param date_regex   Regex returning the date contained in the filename
    #
    # @return Date instance, representing the parsed date
    #
    def FileUtils.date_from_filename(filename, date_regex=/\d{4}-\d{2}-\d{2}/)
      # match the date in the filename
      match    = filename.match(date_regex)
      date_str = match&.captures&.first || match&.to_s

      # if nothing could be match, immediately fail
      raise "Invalid date_regex or filename format" if not date_str

      # regular date
      begin
        if date_str.include?("-")
          Date.parse(date_str)
        # timestamp
        else
          DateTime.strptime(date_str, "%s").to_date
        end
      rescue
        raise "Date format not supported"
      end
    end

    #
    # Parse the extension in a filename
    #
    # @param filename     String containing the filename to be parsed
    #
    # @return String containing the extension of the filename if relevant, None otherwise
    #
    def FileUtils.extension_from_filename(filename)
      if filename.include?('.')
        '.' + filename.split('/').last.split('.')[1..-1].join('.')
      end
    end

    #
    # Get the list of files in the specified directory
    #
    # @param directory   String containing the path to the directory
    #
    # @return array of filenames, in ascending date order
    #
    def FileUtils.files_in_directory(directory)
      Dir.entries(directory).select { |f| !File.directory? f }.sort
    rescue
      raise "Invalid directory #{directory}"
    end

  end

end
