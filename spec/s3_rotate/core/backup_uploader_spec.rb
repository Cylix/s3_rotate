# standard
require 'date'

# 3rd part
require 'fog-aws'

# s3_rotate
require File.expand_path("../../../../lib/s3_rotate/core/backup_uploader", __FILE__)
require File.expand_path("../../../../lib/s3_rotate/utils/file_utils", __FILE__)

describe S3Rotate::BackupUploader do

  before :each do
    @client   = S3Rotate::S3Client.new('key', 'secret', 'bucket', 'region')
    @uploader = S3Rotate::BackupUploader.new(@client)
  end

  describe '#initialize' do

    it 'sets the client' do
      expect(@uploader.s3_client).not_to eq nil
    end

  end

  describe '#upload' do

    it 'uploads all the new files until reaching an already uploaded file' do
      # mock
      @client.connection.directories.get('bucket').files.create(key: '/backup_name/daily/2020-01-02.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: '/backup_name/daily/2020-01-03.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: '/backup_name/daily/2020-01-04.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: '/backup_name/weekly/2020-01-05.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: '/backup_name/monthly/2020-01-06.tgz', body: 'some data')
      @client.connection.directories.get('bucket').files.create(key: '/other_backup_name/daily/2020-01-07.tgz', body: 'some data')

      allow(S3Rotate::FileUtils).to receive(:date_from_filename).and_call_original
      allow(S3Rotate::FileUtils).to receive(:extension_from_filename).and_call_original
      allow(S3Rotate::FileUtils).to receive(:files_in_directory).with('/path/to/dir').and_return([
        'some-backup-2020-01-01.tgz',
        'some-backup-2020-01-02.tgz',
        'some-backup-2020-01-03.tgz',
        'some-backup-2020-01-04.tgz',
        'some-backup-2020-01-05.tgz',
        'some-backup-2020-01-06.tgz',
        'some-backup-2020-01-07.tgz'
      ])
      allow(@client).to receive(:upload).and_return nil
      allow(File).to receive(:open).and_return "raw_data"

      # perform test
      @uploader.upload('backup_name', '/path/to/dir')

      # verify result
      expect(@client).to have_received(:upload).exactly(3)
      expect(@client).to have_received(:upload).with('backup_name', Date.new(2020, 1, 7), 'daily', '.tgz', 'raw_data')
      expect(@client).to have_received(:upload).with('backup_name', Date.new(2020, 1, 6), 'daily', '.tgz', 'raw_data')
      expect(@client).to have_received(:upload).with('backup_name', Date.new(2020, 1, 5), 'daily', '.tgz', 'raw_data')

      expect(S3Rotate::FileUtils).to have_received(:date_from_filename).exactly(4)
      expect(S3Rotate::FileUtils).to have_received(:date_from_filename).with('some-backup-2020-01-07.tgz', /\d{4}-\d{2}-\d{2}/, "%Y-%m-%d")
      expect(S3Rotate::FileUtils).to have_received(:date_from_filename).with('some-backup-2020-01-06.tgz', /\d{4}-\d{2}-\d{2}/, "%Y-%m-%d")
      expect(S3Rotate::FileUtils).to have_received(:date_from_filename).with('some-backup-2020-01-05.tgz', /\d{4}-\d{2}-\d{2}/, "%Y-%m-%d")
      expect(S3Rotate::FileUtils).to have_received(:date_from_filename).with('some-backup-2020-01-04.tgz', /\d{4}-\d{2}-\d{2}/, "%Y-%m-%d")

      expect(S3Rotate::FileUtils).to have_received(:extension_from_filename).exactly(4)
      expect(S3Rotate::FileUtils).to have_received(:extension_from_filename).with('some-backup-2020-01-07.tgz')
      expect(S3Rotate::FileUtils).to have_received(:extension_from_filename).with('some-backup-2020-01-06.tgz')
      expect(S3Rotate::FileUtils).to have_received(:extension_from_filename).with('some-backup-2020-01-05.tgz')
      expect(S3Rotate::FileUtils).to have_received(:extension_from_filename).with('some-backup-2020-01-04.tgz')
    end

    it 'uploads files without extensions' do
      # mock
      allow(S3Rotate::FileUtils).to receive(:date_from_filename).and_call_original
      allow(S3Rotate::FileUtils).to receive(:extension_from_filename).and_return nil
      allow(S3Rotate::FileUtils).to receive(:files_in_directory).with('/path/to/dir').and_return([
        'some-backup-2020-01-01.tgz',
        'some-backup-2020-01-02.tgz',
        'some-backup-2020-01-03.tgz',
      ])
      allow(@client).to receive(:upload).and_return nil
      allow(File).to receive(:open).and_return "raw_data"

      # perform test
      @uploader.upload('backup_name', '/path/to/dir')

      # verify result
      expect(@client).to have_received(:upload).exactly(3)
      expect(@client).to have_received(:upload).with('backup_name', Date.new(2020, 1, 3), 'daily', nil, 'raw_data')
      expect(@client).to have_received(:upload).with('backup_name', Date.new(2020, 1, 2), 'daily', nil, 'raw_data')
      expect(@client).to have_received(:upload).with('backup_name', Date.new(2020, 1, 1), 'daily', nil, 'raw_data')

      expect(S3Rotate::FileUtils).to have_received(:date_from_filename).exactly(3)
      expect(S3Rotate::FileUtils).to have_received(:date_from_filename).with('some-backup-2020-01-03.tgz', /\d{4}-\d{2}-\d{2}/, "%Y-%m-%d")
      expect(S3Rotate::FileUtils).to have_received(:date_from_filename).with('some-backup-2020-01-02.tgz', /\d{4}-\d{2}-\d{2}/, "%Y-%m-%d")
      expect(S3Rotate::FileUtils).to have_received(:date_from_filename).with('some-backup-2020-01-01.tgz', /\d{4}-\d{2}-\d{2}/, "%Y-%m-%d")

      expect(S3Rotate::FileUtils).to have_received(:extension_from_filename).exactly(3)
      expect(S3Rotate::FileUtils).to have_received(:extension_from_filename).with('some-backup-2020-01-03.tgz')
      expect(S3Rotate::FileUtils).to have_received(:extension_from_filename).with('some-backup-2020-01-02.tgz')
      expect(S3Rotate::FileUtils).to have_received(:extension_from_filename).with('some-backup-2020-01-01.tgz')
    end

    it 'does not upload files with broken date' do
      # mock
      original_date_from_filename = S3Rotate::FileUtils.method(:date_from_filename)
      allow(S3Rotate::FileUtils).to receive(:date_from_filename) do |filename, date_regex|
        if filename == 'some-backup-2020-01-02.tgz'
          nil
        else
          original_date_from_filename.call(filename, date_regex)
        end
      end
      allow(S3Rotate::FileUtils).to receive(:extension_from_filename).and_call_original
      allow(S3Rotate::FileUtils).to receive(:files_in_directory).with('/path/to/dir').and_return([
        'some-backup-2020-01-01.tgz',
        'some-backup-2020-01-02.tgz',
        'some-backup-2020-01-03.tgz',
      ])
      allow(@client).to receive(:upload).and_return nil
      allow(File).to receive(:open).and_return "raw_data"

      # perform test
      @uploader.upload('backup_name', '/path/to/dir')

      # verify result
      expect(@client).to have_received(:upload).exactly(2)
      expect(@client).to have_received(:upload).with('backup_name', Date.new(2020, 1, 3), 'daily', '.tgz', 'raw_data')
      expect(@client).to have_received(:upload).with('backup_name', Date.new(2020, 1, 1), 'daily', '.tgz', 'raw_data')

      expect(S3Rotate::FileUtils).to have_received(:date_from_filename).exactly(3)
      expect(S3Rotate::FileUtils).to have_received(:date_from_filename).with('some-backup-2020-01-03.tgz', /\d{4}-\d{2}-\d{2}/, "%Y-%m-%d")
      expect(S3Rotate::FileUtils).to have_received(:date_from_filename).with('some-backup-2020-01-02.tgz', /\d{4}-\d{2}-\d{2}/, "%Y-%m-%d")
      expect(S3Rotate::FileUtils).to have_received(:date_from_filename).with('some-backup-2020-01-01.tgz', /\d{4}-\d{2}-\d{2}/, "%Y-%m-%d")

      expect(S3Rotate::FileUtils).to have_received(:extension_from_filename).exactly(3)
      expect(S3Rotate::FileUtils).to have_received(:extension_from_filename).with('some-backup-2020-01-03.tgz')
      expect(S3Rotate::FileUtils).to have_received(:extension_from_filename).with('some-backup-2020-01-02.tgz')
      expect(S3Rotate::FileUtils).to have_received(:extension_from_filename).with('some-backup-2020-01-01.tgz')
    end

  end

end
