module S3Rotate

  module Logging

    def logger
      Logging.logger
    end

    def self.logger
      @logger ||= Logger.new(STDOUT)
    end

    def self.level(level)
      logger.level = level
    end

  end

end
