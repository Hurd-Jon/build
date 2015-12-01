require 'aws-sdk-v1'
require "awesome_print"

BUCKET_NAME = 'sf-databackup'
FILE_PREFIX = 'rs-ds033190'
FILE_POSTFIX = '.tgz'
$download_available = false
$download_name = ''
$download_date = ''

# filter to identify todays backups
def get_backup_prefix()
   date = Time.now
   formatted_date = date.strftime('%Y-%m-%d')
   bucket_prefix = "FILE_PREFIX_#{formatted_date}"
   # override filter for testing - no files for today
   bucket_prefix = "#{FILE_PREFIX}_2015-11-27"
end

# store details for backup candidate in global variables
def set_backup_data(file, file_date)
   puts "Download candidate found #{file.key}"
   $download_available = true
   $download_name = file.key
   $download_date = file_date
end

# There can be multiple backups in a day - make sure we get the lates
def check_backup(file)
  puts "Checking #{file.key} "
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
puts '------------- If a candidate is found it will be restored  -------------------'
puts '------------- to the mongo instance running on this machine ------------------'

s3 = AWS::S3.new
backup_bucket = s3.buckets[BUCKET_NAME]
backup_prefix = get_backup_prefix()

puts "Expected Prefix = #{backup_prefix}"

backup_bucket.objects.with_prefix(backup_prefix).each do |file|
  check_backup(file)
end

if $download_available
  client = Aws::S3::Client.new
  puts "Downloading #{$download_name}"
  File.open($download_name, 'wb', :encoding => 'BINARY') do |file|
    reap = client.get_object({ bucket: BUCKET_NAME, key: $download_name }, target: file)
  end
end

puts '------------- Finished checking for new mongo file dumps ---------------------'
