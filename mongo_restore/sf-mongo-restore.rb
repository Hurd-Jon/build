require 'aws-sdk-v1'
require "awesome_print"

BUCKET_NAME = 'sf-databackup'
FOLDER = 'mongolab/'
FILE_PREFIX = "#{FOLDER}rs-ds033190"
FILE_POSTFIX = '.tgz'
$download_available = false
$download_name = ''
$download_date = ''
$downloaded_folder_name = ''

# filter to identify todays backups
def get_backup_prefix(backup_date)
   formatted_date = backup_date.strftime('%Y-%m-%d')
   "#{FILE_PREFIX}_#{formatted_date}"
end

# store details for backup candidate in global variables
def set_backup_data(file, file_date)
   puts "-- Download candidate found #{file.key}"
   $download_available = true
   $download_full_name = file.key
   $download_file_name = $download_full_name.gsub(FOLDER,'')
   $download_date = file_date
   $download_folder_name = $download_file_name.gsub(FILE_POSTFIX,'')
end

# There can be multiple backups in a day - make sure we get the latest
def check_backup(file)
   # we are only interested in .tgz files
  if file.key.end_with? FILE_POSTFIX
    puts "-- Checking #{file.key} to see if its the backup we need"
    cleaned_file_name=file.key.gsub(FILE_PREFIX,'').gsub(FILE_POSTFIX,'')
    file_date = Date.parse(cleaned_file_name)

    if $download_available
       if file_date > $download_date
         set_backup_data(file, file_date)
       end
     else
       set_backup_data(file, file_date)
    end
  end
end

puts '------------- Checking for new mongo file dumps ------------------------------'
s3 = AWS::S3.new

puts '-- getting run data from command line arguments'
date_parameter = ARGV[0]
run_date = DateTime.now
if date_parameter == 'USE_TODAY' || date_parameter.empty?
  run_date = DateTime.now
else
  run_date = DateTime.parse(date_parameter)
end

backup_bucket = s3.buckets[BUCKET_NAME]
backup_prefix = get_backup_prefix(run_date)

puts "-- Run date is set to #{run_date}"
puts "-- Looking for files with prefix = #{backup_prefix}"

backup_bucket.objects.with_prefix(backup_prefix).each do |file|
  check_backup(file)
end

if $download_available
  client = AWS::S3::Client.new
  puts "-- Downloading #{$download_full_name} from bucket #{BUCKET_NAME}"
  File.open($download_file_name, 'wb', :encoding => 'BINARY') do |file|
    client.get_object({ bucket_name: BUCKET_NAME.to_s, key: $download_full_name }, target: file) do |chunk|
      file.write(chunk)
    end
  end
  puts "-- Unzipping tar file : #{$download_file_name} --"
  cmd = "tar -xvzf ./#{$download_file_name}"
  system(cmd)
  puts "-- restoring file to mongo #{$download_folder_name} --------------"
  cmd = "mongorestore --drop #{$download_folder_name}"
  system(cmd)
  puts "-- delete the download folder to clean up --"
  cmd= "rm -rf #{$download_folder_name}"
  system(cmd)
  puts "-- delete the tar file to clean up -----"
  cmd = "rm -rf #{$download_file_name}"
  system(cmd)
else
  puts "-- There are no backups to restore at this point, quitting."
end

puts '------------- Finished checking for new mongo file dumps ---------------------'
