# standard
require 'date'

# 3rd part
require 'fog-aws'

# s3_rotate
require File.expand_path("../../../../lib/s3_rotate/core/backup_manager", __FILE__)

describe S3Rotate::BackupManager do

  before :each do
    @manager = S3Rotate::BackupManager.new('key', 'secret', 'bucket', 'region')
  end

  describe '#initialize' do

    it 'sets the client' do
      expect(@manager.s3_client).not_to eq nil
    end

    it 'sets the uploader' do
      expect(@manager.uploader).not_to eq nil
    end

    it 'sets the rotator' do
      expect(@manager.rotator).not_to eq nil
    end

  end

  describe '#upload' do

    it 'calls uploader.upload' do
      # mock
      allow(@manager.uploader).to receive(:upload).with('backup_name', '/path/to/dir', /\d{4}-\d{2}-\d{2}/, "%Y-%m-%d").and_return('upload_result')

      # perform test
      expect(@manager.upload('backup_name', '/path/to/dir', /\d{4}-\d{2}-\d{2}/, "%Y-%m-%d")).to eq 'upload_result'
    end

  end

  describe '#rotate' do

    it 'calls rotator.rotate' do
      # mock
      allow(@manager.rotator).to receive(:rotate).with('backup_name', '/path/to/dir', 1, 2, 3, 4).and_return('rotate_result')

      # perform test
      expect(@manager.rotate('backup_name', '/path/to/dir', 1, 2, 3, 4)).to eq 'rotate_result'
    end

  end

end
