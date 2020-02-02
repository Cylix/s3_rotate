require 'logger'
require 'fog-aws'

require File.expand_path("../../lib/s3_rotate/utils/logging", __FILE__)

RSpec.configure do |c|

  c.before :each do
    Fog.mock!

    fog = Fog::Storage.new(aws_access_key_id: 'key', aws_secret_access_key: 'secret', provider: 'AWS', region: 'region')
    fog.directories.create(key: 'bucket')

    S3Rotate::Logging::level Logger::ERROR
  end

  c.after :each do
    Fog::Mock.reset
    Fog.unmock!
  end

end
