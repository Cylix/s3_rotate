# standard
require 'date'

# s3_rotate
require File.expand_path("../../../../lib/s3_rotate/utils/file_utils", __FILE__)

describe S3Rotate::FileUtils do

  describe '#date_from_filename' do

    it 'parses Date.parse formats' do
      expect(S3Rotate::FileUtils::date_from_filename("/path/to/file-2020-12-13-backup.tar.gz")).to eq Date.new(2020, 12, 13)
    end

    it 'parses timestamp formats' do
      expect(S3Rotate::FileUtils::date_from_filename("/path/to/file-1580098737-backup.tar.gz", /file-(\d+)-backup.tar.gz/, "%s")).to eq Date.new(2020, 1, 27)
    end

    it 'raises if the regex matched nothing' do
      expect{ S3Rotate::FileUtils::date_from_filename("/path/to/file-1580098737-backup.tar.gz", /\d{4}-\d{2}-\d{2}/, "%s") }.to raise_error(RuntimeError, "Invalid date_regex or filename format")
    end

    it 'raises if the matched string can not be parsed' do
      expect{ S3Rotate::FileUtils::date_from_filename("/path/to/file-1580098737-backup.tar.gz", /file-\d+-backup.tar.gz/, "%s") }.to raise_error(RuntimeError, "Invalid date_format")
    end

  end

  describe '#extension_from_filename' do

    it 'handles no extension' do
      expect(S3Rotate::FileUtils::extension_from_filename("backup")).to eq nil
    end

    it 'handles short extension' do
      expect(S3Rotate::FileUtils::extension_from_filename("backup.tgz")).to eq '.tgz'
    end

    it 'handles long extension' do
      expect(S3Rotate::FileUtils::extension_from_filename("backup.tar.gz.1")).to eq '.tar.gz.1'
    end

    it 'handles absolute paths' do
      expect(S3Rotate::FileUtils::extension_from_filename("/path/to.file/backup.tar.gz.1")).to eq '.tar.gz.1'
    end

  end

  describe 'files_in_directory' do

    it 'returns the directory files ordered ASC' do
      expect(S3Rotate::FileUtils::files_in_directory("#{__dir__}/mock")).to eq [ "backup-2020-01-02.test", "backup-2020-01-03.test", "backup-2020-02-01.test", "backup-2021-01-01.test" ]
    end

    it 'raises for invalid directories' do
      expect{ S3Rotate::FileUtils::files_in_directory("/invalid/path") }.to raise_error(RuntimeError, "Invalid directory /invalid/path")
    end

  end

end
