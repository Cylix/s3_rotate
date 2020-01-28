# standard
require 'date'

# 3rd part
require 'fog-aws'

# s3_rotate
require File.expand_path("../../../../lib/s3_rotate/aws/s3_client", __FILE__)

describe S3Rotate::S3Client do

  before :each do
    @client = S3Rotate::S3Client.new('key', 'secret', 'bucket', 'region')
  end

  describe '#initialize' do

    it 'sets the access_key' do
      expect(@client.access_key).to eq 'key'
    end

    it 'sets the access_secret' do
      expect(@client.access_secret).to eq 'secret'
    end

    it 'sets the bucket_name' do
      expect(@client.bucket_name).to eq 'bucket'
    end

    it 'sets the region' do
      expect(@client.region).to eq 'region'
    end

  end

  describe '#connection' do

    it 'sets the connection when unset' do
      # mock
      @client.connection = nil

      # perform test
      expect(@client.connection).not_to eq nil
    end

    it 'does not set the connection when set' do
      # mock
      @client.connection = "some connection"

      # perform test
      expect(@client.connection).to eq "some connection"
    end

  end

  describe '#bucket' do

    it 'gets the bucket' do
      expect(@client.bucket).not_to eq nil
      expect(@client.bucket.key).to eq 'bucket'
    end

  end

  describe '#remote_backups' do

    before do
      @client.connection.directories.get('bucket').files.create(key: '/backup_name/daily/2020-01-01.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: '/backup_name/daily/2020-01-02.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: '/backup_name/daily/2020-02-03.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: '/backup_name/daily/2021-02-04.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: '/backup_name/weekly/2020-01-03.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: '/backup_name/monthly/2020-01-04.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: '/other_backup_name/daily/2020-01-05.tgz', body: 'some data')
    end

    it 'gets the remote backups' do
      expect(@client.remote_backups('backup_name', 'daily')).not_to eq nil
      expect(@client.remote_backups('backup_name', 'daily').files).not_to eq nil
      expect(@client.remote_backups('backup_name', 'daily').files.length).to eq 4
      expect(@client.remote_backups('backup_name', 'daily').files[0].key).to eq "/backup_name/daily/2020-01-01.tgz"
      expect(@client.remote_backups('backup_name', 'daily').files[1].key).to eq "/backup_name/daily/2020-01-02.tgz"
      expect(@client.remote_backups('backup_name', 'daily').files[2].key).to eq "/backup_name/daily/2020-02-03.tgz"
      expect(@client.remote_backups('backup_name', 'daily').files[3].key).to eq "/backup_name/daily/2021-02-04.tgz"
    end

  end

  describe '#exists?' do

    before do
      @client.connection.directories.get('bucket').files.create(key: '/backup_name/daily/2020-01-01.tgz', body: 'some data')
    end

    it 'returns true for existing backups' do
      expect(@client.exists?('backup_name', Date.new(2020, 1, 1), 'daily', '.tgz')).to eq true
    end

    it 'returns false for wrong extension' do
      expect(@client.exists?('backup_name', Date.new(2020, 1, 1), 'daily', '.tar.gz')).to eq false
    end

    it 'returns false for wrong type' do
      expect(@client.exists?('backup_name', Date.new(2020, 1, 1), 'weekly', '.tgz')).to eq false
    end

    it 'returns false for wrong date' do
      expect(@client.exists?('backup_name', Date.new(2020, 1, 2), 'daily', '.tgz')).to eq false
    end

    it 'returns false for backup name' do
      expect(@client.exists?('other_backup_name', Date.new(2020, 1, 1), 'daily', '.tgz')).to eq false
    end

  end

  describe '#upload' do

    it 'uploads files' do
      @client.upload('backup_name', Date.new(2020, 1, 1), 'daily', '.tgz', 'hello world')
      expect(@client.connection.directories.get('bucket', prefix: '/backup_name/daily/2020-01-01.tgz').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: '/backup_name/daily/2020-01-01.tgz').files.first.key).to eq '/backup_name/daily/2020-01-01.tgz'
      expect(@client.connection.directories.get('bucket', prefix: '/backup_name/daily/2020-01-01.tgz').files.first.body).to eq 'hello world'
    end

  end

end
