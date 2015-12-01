
require 'aws-sdk-v1'
require "awesome_print"

BUCKET_NAME = 'sf-databackup'
FILE_PREFIX = 'rs-ds033190'
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
   $download_name = file.key
   $download_date = file_date
   $downloaded_folder_name = file.key.gsub(FILE_POSTFIX,'')
end

# There can be multiple backups in a day - make sure we get the lates
def check_backup(file)
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

#run_date = DateTime.new(2015,11,27)
backup_bucket = s3.buckets[BUCKET_NAME]
backup_prefix = get_backup_prefix(run_date)

puts "-- Run date is set to #{run_date}"
puts "-- Looking for files with prefix = #{backup_prefix}"

backup_bucket.objects.with_prefix(backup_prefix).each do |file|
  check_backup(file)
end

if $download_available
  client = AWS::S3::Client.new
  puts "-- Downloading #{$download_name} from bucket #{BUCKET_NAME}"
  File.open($download_name, 'wb', :encoding => 'BINARY') do |file|
    client.get_object({ bucket_name: BUCKET_NAME.to_s, key: $download_name }, target: file) do |chunk|
      file.write(chunk)
    end
  end
  puts "-- Unzipping tar file : #{$download_name} --"
  cmd = "tar -xvzf ./#{$download_name}"
  system(cmd)
  puts "-- restoring file to mongo --------------"
  cmd = "mongorestore --drop ./#{$downloaded_folder_name}"
  system(cmd)
  puts "-- delete the dump folder to clean up --"
  cmd= "rm -rf #{$downloaded_folder_name}"
  system(cmd)
  puts "-- delete the tar file to clean up -----"
  cmd = "rm -rf #{$download_name}"
  system(cmd)
else
  puts "-- There are no backups to restore at this point, quitting."
end

puts '------------- Finished checking for new mongo file dumps ---------------------'