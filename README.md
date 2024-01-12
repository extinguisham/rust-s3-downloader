# Rust S3 Downloader (and optional uploader)
Frustrated with how slow the AWS CLI is for downloading a large number of files for a one-off task, I thought I'd see how far I could get with Rust. This is the result.

There's a lot of work that would need to be done to make it nice to use for someone else but I was still very happy with how much faster it was at performing the task I needed it for (Somewhere between 6 and 10 times faster than the CLI).

## Usage
```shell
# Download all files in the bucket to the default ./files directory and using default AWS credentials
rust-s3-downloader --bucket my-bucket

# Download all files in the bucket prefix to the default ./files directory and using passed in credentials and region
rust-s3-downloader --bucket my-bucket --prefix logs/ --profile default --region us-east-1

# Download all files in bucket and upload any that don't exist to a bucket in another account (essentially aws s3 sync)
rust-s3-downloader --bucket my-bucket --profile account1 --region us-east-1 --upload-bucket my-other-bucket --upload-profile account2 --upload-region ap-southeast-2
```
