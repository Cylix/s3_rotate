RSpec.configure do |c|

  c.before :each do
    Fog.mock!

    fog = Fog::Storage.new(aws_access_key_id: 'key', aws_secret_access_key: 'secret', provider: 'AWS', region: 'region')
    fog.directories.create(key: 'bucket')
  end

  c.after :each do
    Fog::Mock.reset
    Fog.unmock!
  end

end
