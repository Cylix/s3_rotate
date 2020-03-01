# standard
require 'date'
require 'fileutils'

# 3rd part
require 'fog-aws'

# s3_rotate
require File.expand_path("../../../../lib/s3_rotate/core/backup_rotator", __FILE__)
require File.expand_path("../../../../lib/s3_rotate/utils/file_utils", __FILE__)

describe S3Rotate::BackupRotator do

  before :each do
    @client  = S3Rotate::S3Client.new('key', 'secret', 'bucket', 'region')
    @rotator = S3Rotate::BackupRotator.new(@client)
  end

  describe '#initialize' do

    it 'sets the client' do
      expect(@rotator.s3_client).not_to eq nil
    end

  end

  describe '#rotate' do

    it 'calls the different rotate function in the correct order' do
      # mock
      allow(@rotator).to receive(:rotate_local).and_return nil
      allow(@rotator).to receive(:rotate_daily).and_return nil
      allow(@rotator).to receive(:rotate_weekly).and_return nil
      allow(@rotator).to receive(:rotate_monthly).and_return nil

      # perform test
      @rotator.rotate('backup_name', '/path/to/dir', max_local=10, max_daily=11, max_weekly=12, max_monthly=13)

      # verify result
      expect(@rotator).to have_received(:rotate_local).with('/path/to/dir', 10)
      expect(@rotator).to have_received(:rotate_daily).with('backup_name', 11)
      expect(@rotator).to have_received(:rotate_weekly).with('backup_name', 12)
      expect(@rotator).to have_received(:rotate_monthly).with('backup_name', 13)
    end

  end

  describe '#rotate_daily' do

    it 'rotates and cleanup when relevant' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-15.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-16.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-17.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-01-06.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-01-06.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_daily('backup_name', 3)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 3
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-15.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].key).to eq 'backup_name/daily/2020-01-16.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].key).to eq 'backup_name/daily/2020-01-17.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 2
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-01-06.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].key).to eq 'backup_name/weekly/2020-01-13.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].key).to eq 'backup_name/monthly/2020-01-06.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].body).to eq 'some data'
    end

    it 'only cleanup when relevant' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-15.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-16.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-17.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-01-11.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-01-06.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_daily('backup_name', 3)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 3
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-15.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].key).to eq 'backup_name/daily/2020-01-16.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].key).to eq 'backup_name/daily/2020-01-17.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-01-11.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].key).to eq 'backup_name/monthly/2020-01-06.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].body).to eq 'some data'
    end

    it 'only rotates when relevant' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-01-06.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-01-06.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_daily('backup_name', 3)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 3
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].key).to eq 'backup_name/daily/2020-01-13.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].key).to eq 'backup_name/daily/2020-01-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 2
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-01-06.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].key).to eq 'backup_name/weekly/2020-01-13.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].key).to eq 'backup_name/monthly/2020-01-06.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].body).to eq 'some data'
    end

    it 'rotates multiples when relevant' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-16.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-20.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-21.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-24.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-27.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-29.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-01-06.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-01-06.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_daily('backup_name', 3)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 3
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-24.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].key).to eq 'backup_name/daily/2020-01-27.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].key).to eq 'backup_name/daily/2020-01-29.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 4
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-01-06.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].key).to eq 'backup_name/weekly/2020-01-13.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[2].key).to eq 'backup_name/weekly/2020-01-20.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[2].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[3].key).to eq 'backup_name/weekly/2020-01-27.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[3].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].key).to eq 'backup_name/monthly/2020-01-06.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].body).to eq 'some data'
    end

    it 'always promote when there is no weekly backup' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-15.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-16.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-17.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-01-06.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_daily('backup_name', 3)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 3
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-15.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].key).to eq 'backup_name/daily/2020-01-16.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].key).to eq 'backup_name/daily/2020-01-17.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].key).to eq 'backup_name/monthly/2020-01-06.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].body).to eq 'some data'
    end

    it 'does nothing where there is no daily backup' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-01-06.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-01-06.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_daily('backup_name', 3)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 0

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-01-06.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].key).to eq 'backup_name/monthly/2020-01-06.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].body).to eq 'some data'
    end

  end

  describe '#rotate_weekly' do

    it 'rotates and cleanup when relevant' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-15.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-16.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-17.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-07.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-21.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-28.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_weekly('backup_name', 2)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 6
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].key).to eq 'backup_name/daily/2020-01-13.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].key).to eq 'backup_name/daily/2020-01-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].key).to eq 'backup_name/daily/2020-01-15.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].key).to eq 'backup_name/daily/2020-01-16.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].key).to eq 'backup_name/daily/2020-01-17.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 2
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-02-21.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].key).to eq 'backup_name/weekly/2020-02-28.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 2
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].key).to eq 'backup_name/monthly/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[1].key).to eq 'backup_name/monthly/2020-02-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[1].body).to eq 'some data'
    end

    it 'only cleanup when relevant' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-15.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-16.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-17.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-07.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-21.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-28.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-01-29.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_weekly('backup_name', 2)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 6
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].key).to eq 'backup_name/daily/2020-01-13.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].key).to eq 'backup_name/daily/2020-01-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].key).to eq 'backup_name/daily/2020-01-15.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].key).to eq 'backup_name/daily/2020-01-16.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].key).to eq 'backup_name/daily/2020-01-17.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 2
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-02-21.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].key).to eq 'backup_name/weekly/2020-02-28.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].key).to eq 'backup_name/monthly/2020-01-29.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].body).to eq 'some data'
    end

    it 'only rotates when relevant' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-15.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-16.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-17.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-07.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_weekly('backup_name', 2)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 6
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].key).to eq 'backup_name/daily/2020-01-13.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].key).to eq 'backup_name/daily/2020-01-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].key).to eq 'backup_name/daily/2020-01-15.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].key).to eq 'backup_name/daily/2020-01-16.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].key).to eq 'backup_name/daily/2020-01-17.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 2
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-02-07.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].key).to eq 'backup_name/weekly/2020-02-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 2
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].key).to eq 'backup_name/monthly/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[1].key).to eq 'backup_name/monthly/2020-02-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[1].body).to eq 'some data'
    end

    it 'rotates multiple when relevant' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-15.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-16.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-17.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-07.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-03-06.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-03-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-04-05.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-04-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_weekly('backup_name', 2)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 6
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].key).to eq 'backup_name/daily/2020-01-13.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].key).to eq 'backup_name/daily/2020-01-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].key).to eq 'backup_name/daily/2020-01-15.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].key).to eq 'backup_name/daily/2020-01-16.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].key).to eq 'backup_name/daily/2020-01-17.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 2
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-04-05.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].key).to eq 'backup_name/weekly/2020-04-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 3
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].key).to eq 'backup_name/monthly/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[1].key).to eq 'backup_name/monthly/2020-02-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[2].key).to eq 'backup_name/monthly/2020-04-05.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[2].body).to eq 'some data'
    end

    it 'always promote when there is no monthly backup' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-15.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-16.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-17.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-07.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-21.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-28.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_weekly('backup_name', 2)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 6
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].key).to eq 'backup_name/daily/2020-01-13.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].key).to eq 'backup_name/daily/2020-01-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].key).to eq 'backup_name/daily/2020-01-15.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].key).to eq 'backup_name/daily/2020-01-16.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].key).to eq 'backup_name/daily/2020-01-17.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 2
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-02-21.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].key).to eq 'backup_name/weekly/2020-02-28.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].key).to eq 'backup_name/monthly/2020-02-07.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].body).to eq 'some data'
    end

    it 'does nothing where there is no weekly backup' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-15.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-16.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-17.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_weekly('backup_name', 2)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 6
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].key).to eq 'backup_name/daily/2020-01-13.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].key).to eq 'backup_name/daily/2020-01-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].key).to eq 'backup_name/daily/2020-01-15.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].key).to eq 'backup_name/daily/2020-01-16.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].key).to eq 'backup_name/daily/2020-01-17.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 0

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].key).to eq 'backup_name/monthly/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].body).to eq 'some data'
    end

  end

  describe '#rotate_monthly' do

    it 'cleanup when relevant' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-15.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-16.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-17.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-07.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-21.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-28.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-02-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-03-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-04-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_monthly('backup_name', 2)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 6
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].key).to eq 'backup_name/daily/2020-01-13.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].key).to eq 'backup_name/daily/2020-01-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].key).to eq 'backup_name/daily/2020-01-15.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].key).to eq 'backup_name/daily/2020-01-16.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].key).to eq 'backup_name/daily/2020-01-17.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 4
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-02-07.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].key).to eq 'backup_name/weekly/2020-02-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[2].key).to eq 'backup_name/weekly/2020-02-21.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[2].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[3].key).to eq 'backup_name/weekly/2020-02-28.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[3].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 2
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].key).to eq 'backup_name/monthly/2020-03-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[1].key).to eq 'backup_name/monthly/2020-04-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[1].body).to eq 'some data'
    end

    it 'does not cleanup if not relevant' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-15.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-16.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-17.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-07.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-21.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-28.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/monthly/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_monthly('backup_name', 2)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 6
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].key).to eq 'backup_name/daily/2020-01-13.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].key).to eq 'backup_name/daily/2020-01-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].key).to eq 'backup_name/daily/2020-01-15.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].key).to eq 'backup_name/daily/2020-01-16.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].key).to eq 'backup_name/daily/2020-01-17.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 4
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-02-07.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].key).to eq 'backup_name/weekly/2020-02-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[2].key).to eq 'backup_name/weekly/2020-02-21.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[2].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[3].key).to eq 'backup_name/weekly/2020-02-28.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[3].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].key).to eq 'backup_name/monthly/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files[0].body).to eq 'some data'
    end

    it 'does nothing if there is no monthly backup' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-13.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-15.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-16.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-17.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-07.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-14.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-21.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'backup_name/weekly/2020-02-28.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: 'other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      # perform test
      @rotator.rotate_monthly('backup_name', 2)

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 6
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].key).to eq 'backup_name/daily/2020-01-13.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].key).to eq 'backup_name/daily/2020-01-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[2].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].key).to eq 'backup_name/daily/2020-01-15.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[3].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].key).to eq 'backup_name/daily/2020-01-16.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[4].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].key).to eq 'backup_name/daily/2020-01-17.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[5].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 4
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-02-07.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].key).to eq 'backup_name/weekly/2020-02-14.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[1].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[2].key).to eq 'backup_name/weekly/2020-02-21.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[2].body).to eq 'some data'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[3].key).to eq 'backup_name/weekly/2020-02-28.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[3].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/monthly').files.length).to eq 0
    end

  end

  describe '#rotate_local' do

    before :each do
      Dir.mkdir('./tmp')
      FileUtils.touch('./tmp/2020-01-01-backup.tar')
      FileUtils.touch('./tmp/2020-01-02-backup.tar')
      FileUtils.touch('./tmp/2020-01-03-backup.tar')
      FileUtils.touch('./tmp/2020-01-04-backup.tar')
      FileUtils.touch('./tmp/2020-01-05-backup.tar')
    end

    after :each do
      FileUtils.rm_rf('./tmp')
    end

    it 'cleanup if relevant' do
      # perform test
      @rotator.rotate_local('./tmp', 3)

      # verify result
      expect(Dir.entries('./tmp').select { |f| !File.directory? f }.sort).to eq ["2020-01-03-backup.tar", "2020-01-04-backup.tar", "2020-01-05-backup.tar"]
    end

    it 'does not cleanup if not relevant' do
      # perform test
      @rotator.rotate_local('./tmp', 7)

      # verify result
      expect(Dir.entries('./tmp').select { |f| !File.directory? f }.sort).to eq ["2020-01-01-backup.tar", "2020-01-02-backup.tar", "2020-01-03-backup.tar", "2020-01-04-backup.tar", "2020-01-05-backup.tar"]
    end

    it 'does nothing if there is no local backup' do
      # mock
      FileUtils.rm_rf('./tmp')
      Dir.mkdir('./tmp')

      # perform test
      @rotator.rotate_local('./tmp', 7)

      # verify result
      expect(Dir.entries('./tmp').select { |f| !File.directory? f }.sort).to eq []
    end

  end

  describe '#should_promote_daily_to_weekly?' do

    it 'promotes after 7 days' do
      expect(@rotator.should_promote_daily_to_weekly?('/gitlab/daily/2020-01-14.tar', '/gitlab/weekly/2020-01-07.tar')).to eq true
    end

    it 'does not promote before 7 days' do
      expect(@rotator.should_promote_daily_to_weekly?('/gitlab/daily/2020-01-13.tar', '/gitlab/weekly/2020-01-07.tar')).to eq false
    end

    it 'does not promote if no daily file' do
      expect(@rotator.should_promote_daily_to_weekly?(nil, '/gitlab/weekly/2020-01-07.tar')).to eq false
    end

    it 'promotes if no weekly file' do
      expect(@rotator.should_promote_daily_to_weekly?('/gitlab/daily/2020-01-13.tar', nil)).to eq true
    end

  end

  describe '#should_promote_weekly_to_monthly?' do

    it 'promotes after one month' do
      expect(@rotator.should_promote_weekly_to_monthly?('/gitlab/weekly/2020-02-14.tar', '/gitlab/monthly/2020-01-14.tar')).to eq true
    end

    it 'does not promote before one month' do
      expect(@rotator.should_promote_weekly_to_monthly?('/gitlab/weekly/2020-02-13.tar', '/gitlab/monthly/2020-01-14.tar')).to eq false
    end

    it 'does not promote if no weekly file' do
      expect(@rotator.should_promote_weekly_to_monthly?(nil, '/gitlab/monthly/2020-01-07.tar')).to eq false
    end

    it 'promotes if no monthly file' do
      expect(@rotator.should_promote_weekly_to_monthly?('/gitlab/weekly/2020-01-13.tar', nil)).to eq true
    end

  end

  describe '#promote' do

    it 'promotes backup' do
      # mock data
      file = @client.connection.directories.get('bucket').files.create(key: 'backup_name/daily/2020-01-12.tgz', body: 'some data')

      # perform test
      @rotator.promote('backup_name', file, 'weekly')

      # verify result
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].key).to eq 'backup_name/weekly/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/weekly').files[0].body).to eq 'some data'

      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files.length).to eq 1
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].key).to eq 'backup_name/daily/2020-01-12.tgz'
      expect(@client.connection.directories.get('bucket', prefix: 'backup_name/daily').files[0].body).to eq 'some data'
    end

  end

end
