# S3 Rotate
`S3 Rotate` provides easy backup rotation management on Amazon AWS S3.

`S3 Rotate` was developed to solve the following issues:
- be able to easily and automatically upload all kind of backup files to S3
- be able to safely and automatically cleanup old backup files on the local machine to prevent running out of disk space
- be able to safely and automatically rotate & cleanup old backup files on S3 to keep only what matters



## Requirements
- Ruby >= 2.0.0


## Installation
In your `Gemfile`:
```bash
gem install s3_rotate
```

In your `file.rb`
```ruby
require 's3_rotate'
```



## Use Case
Let's say you have two services for which you generate daily backups:
- A Gitlab server, generating daily backups under `/var/opt/gitlab/backups/` with the following format `1578804325_2020_01_11_12.6.2_gitlab_backup.tar` (`timestamp_YYYY_MM_dd_version_gitlab_backup.tar`)
- A DefectDojo server, generating daily backups under `/data/defect-dojo/backups` with the following format `dojo-2020_01_25.sql` (`dojo-YYYY_MM_dd.tar.gz`)

In particular, let's say you have the following on your host machine:
```
$> ls -l /var/opt/gitlab/backups/
-rw-r--r-- 1 git git 3754987520 Jan 11 20:46 1578804325_2020_01_11_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3754936320 Jan 12 20:45 1578890716_2020_01_12_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3765555200 Jan 13 20:47 1578977175_2020_01_13_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3765207040 Jan 14 20:52 1579063838_2020_01_14_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3803136000 Jan 15 20:53 1579150281_2020_01_15_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3819448320 Jan 16 20:49 1579236516_2020_01_16_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3819223040 Jan 17 20:53 1579323130_2020_01_17_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3819151360 Jan 18 20:50 1579409341_2020_01_18_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3819499520 Jan 19 20:50 1579495780_2020_01_19_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3816120320 Jan 20 20:48 1579582055_2020_01_20_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3818106880 Jan 21 20:54 1579668786_2020_01_21_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3815270400 Jan 22 20:50 1579754971_2020_01_22_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3813365760 Jan 23 20:50 1579841383_2020_01_23_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3814205440 Jan 24 20:48 1579927638_2020_01_24_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3814236160 Jan 25 20:52 1580014249_2020_01_25_12.6.2_gitlab_backup.tar
-rw-r--r-- 1 git git 3818567680 Jan 26 20:45 1580100263_2020_01_26_12.6.2_gitlab_backup.tar


$> ls -l /data/defect-dojo/backups
-rw-r--r-- 1 root root 226529 Jan 25 20:15 dojo-2020_01_25.sql
-rw-r--r-- 1 root root 226529 Jan 26 20:15 dojo-2020_01_26.sql
```

If left unmanaged, you may run into a couple of issues:
- `Number of Backups`: The number of backups will quickly grow, consuming disk space and requiring manual operation to cleanup the old backups
- `Size of Backups`: The size of each individual backup file will continuously increase, resulting in more frequent manual operations
- `Hardware Failures`: The backups are currently only located on one machine, making them vulnerable to hardware failures
- `Additional Backups`: Additional services backups may need to be handled on the long-run, each having a different way to generate backups (different disk location, different naming, ...)

The upload feature of `S3 Rotate` will upload to S3 any new backup file for Gitlab, DefectDojo, or anything else.
The newly uploaded backups are categorized as `daily`.

The rotate feature of `S3 Rotate` is more interesting and will do the following:
- `local rotate`: You can specify how many `local` backups you want to keep. Only the most recent ones within your limit will remain, the other ones will be deleted.
- `daily rotate`: Every week, one of the `daily` backup on AWS S3 is converted into a `weekly` backup on AWS S3. Additionally, you can specify how many `daily` backups you want to keep. Only the most recent ones within your limit will remain, the other ones will be deleted.
- `weekly rotate`: Every month, one of the `weekly` backup on AWS S3 is converted into a `monthly` backup on AWS S3. Additionally, you can specify how many `weekly` backups you want to keep. Only the most recent ones within your limit will remain, the other ones will be deleted.
- `monthly rotate`: You can specify how many `monthly` backups you want to keep. Only the most recent ones within your limit will remain, the other ones will be deleted.

For example, if you have the following:
- one backup every day from January 1st to March 31st
- only want to keep the 7 most recent backups locally
- only want to keep the 14 most recent daily backups on AWS S3
- only want to keep the 4 most recent weekly backups on AWS S3
- only want to keep the 3 most recent monthly backups on AWS S3

`S3 Rotate` will do the following:
- Every day, your new backup will be uploaded as a new daily backup on AWS S3
- If you have more than 7 backups locally, the oldest ones will be removed until you got 7 left
- If your most recent weekly backup is one week apart from one of your daily backup, that daily backup will be promoted into a weekly backup
- If you have more than 14 daily backups on AWS S3, the oldest ones will be removed until you got 7 left
- If your most recent monthly backup is one month apart from one of your weekly backup, that weekly backup will be promoted into a monthly backup
- If you have more than 4 weekly backups on AWS S3, the oldest ones will be removed until you got 4 left
- If you have more than 3 monthly backups on AWS S3, the oldest ones will be removed until you got 3 left

With this configuration, on March 31st, your local server will look like:
```
  /var/opt/gitlab/backups/ # 14 most recent local backups
    timestamp-2020_03_31_12.6.2_gitlab_backup.tar
    timestamp-2020_03_30_12.6.2_gitlab_backup.tar
    timestamp-2020_03_29_12.6.2_gitlab_backup.tar
    timestamp-2020_03_28_12.6.2_gitlab_backup.tar
    timestamp-2020_03_27_12.6.2_gitlab_backup.tar
    timestamp-2020_03_26_12.6.2_gitlab_backup.tar
    timestamp-2020_03_25_12.6.2_gitlab_backup.tar
    timestamp-2020_03_24_12.6.2_gitlab_backup.tar
    timestamp-2020_03_23_12.6.2_gitlab_backup.tar
    timestamp-2020_03_22_12.6.2_gitlab_backup.tar
    timestamp-2020_03_21_12.6.2_gitlab_backup.tar
    timestamp-2020_03_20_12.6.2_gitlab_backup.tar
    timestamp-2020_03_19_12.6.2_gitlab_backup.tar
    timestamp-2020_03_18_12.6.2_gitlab_backup.tar
```

With this configuration, on March 31st, your AWS S3 bucket will look like:
```
  bucket/
    gitlab/
      daily/ # 7 most recent daily backups
        2020-03-31.tar
        2020-03-30.tar
        2020-03-29.tar
        2020-03-28.tar
        2020-03-27.tar
        2020-03-26.tar
        2020-03-25.tar

      weekly/ # 4 most recent weekly backups
        2020-03-25.tar
        2020-03-18.tar
        2020-03-11.tar
        2020-03-04.tar

      monthly/ # 3 most recent local backups
        2020-03-01.tar
        2020-02-01.tar
        2020-01-01.tar

    defect-dojo/
      [...]
    [...]
```

## Usage
### S3Rotate::BackupManager.upload
Prototype:
```ruby
#
# Upload local backup files to AWS S3
# Only uploads new backups
# Only uploads backups as daily backups: use `rotate` to generate the weekly & monthly files
#
# @param backup_name        String containing the name of the backup to upload
# @param local_backups_path String containing the path to the directory containing the backups
# @param date_regex         Regex returning the date contained in the filename of each backup
#
# @return nothing
#
def upload(backup_name, local_backups_path, date_regex=/\d{4}-\d{2}-\d{2}/)
```

Example:
```ruby
require 's3_rotate'

backup_manager = S3Rotate::BackupManager.new(aws_access_key_id, aws_secret_access_key, bucket_name, region)
backup_manager.upload("defect-dojo-backup", "/var/opt/defect-dojo/backups")
```

`S3Rotate::BackupManager.upload(backup_name, local_backups_path, date_regex)` uploads the new backups from a local directory to the server.
- `backup_name`: This is how you want to name your group of backup. This will be used to create a directory on the AWS S3 bucket under which your backup will be stored. It can be anything you want
- `local_backups_path`: This is the path to the local directory containing your backups to be uploaded
- `date_regex`: To rotate backups from daily to weekly, and from weekly to monthly, `S3 Backup` needs to determine which date is related to each backup file. This is done by extracting the date information from the filename, using a regex specified in `date_regex`.

`date_regex` is the most important part here: without it, `S3 Rotate` does not know when your backup was generated and can not rotate your backups properly. Here are some examples of regex:
- if your backups are like `1578804325_2020_01_11_12.6.2_gitlab_backup.tar`, you can use `date_regex=/\d{4}-\d{2}-\d{2}/` (this will match `2020_01_11_12`)
- if your backups are like `1578804325_gitlab_backup.tar`, you can use `date_regex=/(\d+)_gitlab_backup.tar/` (this will match `1578804325`)

As of now, `date_regex` can be:
- any string that can be parsed by `Date.parse`
- a timestamp

### S3Rotate::BackupManager.rotate
Prototype:
```ruby
#
# Rotate files (local, daily, weekly, monthly) and apply maximum limits for each type
#
# @param backup_name        String containing the name of the backup to rotate
# @param local_backups_path String containing the path to the directory containing the backups
# @param max_local          Integer specifying the maximum number of local backups to keep
# @param max_daily          Integer specifying the maximum number of daily backups to keep
# @param max_weekly         Integer specifying the maximum number of weekly backups to keep
# @param max_monthly        Integer specifying the maximum number of monthly backups to keep
#
# @return nothing
#
def rotate(backup_name, local_backups_dir, max_local=3, max_daily=7, max_weekly=4, max_monthly=3)
```

Example:
```ruby
require 's3_rotate'

backup_manager = S3Rotate::BackupManager.new(aws_access_key_id, aws_secret_access_key, bucket_name, region)
backup_manager.rotate("defect-dojo-backup", "/var/opt/defect-dojo/backups")
```

`S3Rotate::BackupManager.rotate` handles the backup rotation detailed previously in `Use Case`.
- `backup_name`: This is how you want to name your group of backup. This will be used to create a directory on the AWS S3 bucket under which your backup will be stored. It can be anything you want
- `local_backups_path`: This is the path to the local directory containing your backups to be rotated
- `max_local`: This is the maximum number of local backups you want to keep.
- `max_daily`: This is the maximum number of AWS S3 daily backups you want to keep.
- `max_weekly`: This is the maximum number of AWS S3 weekly backups you want to keep.
- `max_monthly`: This is the maximum number of AWS S3 monthly backups you want to keep.



## Tests
This gem has 100% tests coverage.

You can run all the tests with:
```bash
$> bundle exec rspec .
```


## Areas of improvements
- Days are currently the smallest unit of time, but it could be interesting to provide rotation per hours.
- Backups stored on S3 only have 3 types of information: the name of the backup, the type of backup (daily, weekly, monthly) and the date of the backup. It could be interesting to retain more information (for example, the version of the service that generated the backup). It is currently possible to do it using the backup name configuration, but it is not optimal in all situations.
- There is currently no way to disable rotation & cleanup for one unit of time (days, weeks, months), except by setting an arbitrarily high maximum number of files to retain.
- S3 Rotate only supports AWS S3, while it could be interesting to support additional providers.



## Author
[Simon Ninon](https://github.com/Cylix)



## License
[MIT License](LICENSE)



## Contribute
1. Fork
2. Create your branch (`git checkout -b my-branch`)
3. Commit your new features (`git commit -am 'New features'`)
4. Push (`git push origin my-branch`)
5. Make a `Pull request`
