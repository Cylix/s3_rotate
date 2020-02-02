require 'date'

module S3Rotate

  module FileUtils

    #
    # Parse the date in a filename
    # Date can be any format recognize by Date.parse, or be a timestamp
    #
    # @param filename     String containing the filename to be parsed.
    # @param date_regex   Regex returning the date contained in the filename
    # @param date_format  Format to be used by DateTime.strptime to parse the extracted date
    #
    # @return Date instance, representing the parsed date
    #
    def FileUtils.date_from_filename(filename, date_regex=/\d{4}-\d{2}-\d{2}/, date_format="%Y-%m-%d")
      # match the date in the filename
      match = filename.match(date_regex)

      if not match
        date_str = nil
      elsif not match.captures
        date_str = match.to_s
      else
        date_str = match.captures.first || match.to_s
      end

      # if nothing could be match, immediately fail
      raise "Invalid date_regex or filename format" if not date_str

      # regular date
      begin
        DateTime.strptime(date_str, date_format).to_date
      rescue
        raise "Invalid date_format"
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
