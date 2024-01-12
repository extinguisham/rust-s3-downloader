use aws_config::meta::region::RegionProviderChain;
use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::Region;
use aws_sdk_s3::types::Object;
use aws_sdk_s3::Client;
use clap::Parser;
use futures::future::join_all;
use glob::glob;
use std::collections::HashSet;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;

const MAX_CONCURRENT_OPERATIONS: usize = 30;

#[derive(Debug, Error)]
enum Error {
    #[error("io error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("s3 error: {0}")]
    S3Error(#[from] aws_sdk_s3::Error),
    #[error("s3 put object error: {0}")]
    PutObjectError(
        #[from]
        aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_s3::operation::put_object::PutObjectError,
            aws_smithy_runtime_api::http::Response,
        >,
    ),
    #[error("s3 get object error: {0}")]
    GetObjectError(
        #[from]
        aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_s3::operation::get_object::GetObjectError,
            aws_smithy_runtime_api::http::Response,
        >,
    ),
    #[error("s3 list objects error: {0}")]
    ListObjectsError(
        #[from]
        aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error,
            aws_smithy_runtime_api::http::Response,
        >,
    ),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct ObjectKey {
    key: String,
}

impl<'a> From<&'a Object> for ObjectKey {
    fn from(object: &'a Object) -> Self {
        ObjectKey {
            key: object.key.as_deref().unwrap_or_default().to_string(),
        }
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)] // Read from `Cargo.toml`
struct Cli {
    #[arg(short, long)]
    bucket: String,
    #[arg(long)]
    prefix: Option<String>,
    #[arg(short, long)]
    profile: Option<String>,
    #[arg(short, long)]
    region: Option<String>,
    #[arg(short = 'p', long, default_value_t = String::from("./files"))]
    download_path: String, // Is there a better path option than string?
    #[arg(long, requires = "input")]
    upload_bucket: Option<String>,
    #[arg(long)]
    upload_prefix: Option<String>,
    #[arg(long, group = "input")]
    upload_profile: Option<String>,
    #[arg(long, group = "input")]
    upload_region: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();

    println!("Setting up AWS download client...");
    let download_client = create_client(cli.region, cli.profile).await;

    println!("Obtaining list of {} objects...", cli.bucket);
    let download_objects =
        list_all_objects(&download_client, &cli.bucket, cli.prefix.clone()).await?;
    println!("Found {} objects", download_objects.len());

    match cli.upload_bucket {
        Some(bucket) => {
            if cli.upload_profile.is_none() || cli.upload_region.is_none() {
                println!("Upload bucket specified, but no upload profile or region specified. Skipping upload...");
                return Ok(());
            }
            println!("Setting up AWS upload client...");
            let upload_client = create_client(cli.upload_region, cli.upload_profile).await;
            println!("Obtaining list of {:?} objects...", bucket);
            let upload_objects = list_all_objects(&upload_client, &bucket, cli.prefix).await?;
            println!("Found {} objects", download_objects.len());

            println!("Diffing the results...");
            let missing_items = find_missing_items(&download_objects, &upload_objects).await;
            println!("Downloading missing items...");
            get_missing_objects(
                &download_client,
                &cli.bucket,
                missing_items,
                cli.download_path.clone(),
            )
            .await?;

            println!("Uploading missing items...");
            upload_missing_objects(&upload_client, &bucket, cli.download_path.clone()).await?;
        }
        None => {
            let p = match cli.prefix.clone() {
                Some(p) => p,
                None => "".to_string(),
            };
            println!(
                "No upload bucket specified, downloading everything from {}/{}",
                cli.bucket, p
            );
            download_all_objects(
                &download_client,
                &cli.bucket,
                download_objects,
                cli.download_path.clone(),
            )
            .await?;
        }
    }

    Ok(())
}

async fn create_client(region: Option<String>, profile_name: Option<String>) -> Client {
    let region = get_region(region).await;
    println!("Using region: {}", region);
    let credentials_provider = create_credentials_provider(profile_name).await;
    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .region(region)
        .load()
        .await;
    Client::new(&config)
}

async fn get_region(region: Option<String>) -> Region {
    let default_region = RegionProviderChain::default_provider()
        .region()
        .await
        .unwrap()
        .to_string();
    let region_str = region.unwrap_or_else(|| default_region);
    Region::new(region_str)
}

async fn create_credentials_provider(
    profile_name: Option<String>,
) -> ProfileFileCredentialsProvider {
    match profile_name {
        Some(profile_name) => ProfileFileCredentialsProvider::builder()
            .profile_name(profile_name)
            .build(),
        None => ProfileFileCredentialsProvider::builder().build(),
    }
}

async fn list_all_objects(
    client: &Client,
    bucket: &str,
    prefix: Option<String>,
) -> Result<Vec<Object>, Error> {
    let mut continuation_token: Option<String> = None;
    let mut all_objects = Vec::new();

    loop {
        let resp = match prefix.as_ref() {
            Some(p) => {
                client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix(p)
                    .set_continuation_token(continuation_token)
                    .send()
                    .await?
            }
            None => {
                client
                    .list_objects_v2()
                    .bucket(bucket)
                    .set_continuation_token(continuation_token)
                    .send()
                    .await?
            }
        };

        for object in resp.contents() {
            all_objects.push(object.clone())
        }

        if let Some(is_truncated) = resp.is_truncated {
            if is_truncated {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
                continue;
            }
        }
        break;
    }

    Ok(all_objects)
}

async fn find_missing_items<'a>(
    old_bucket_items: &'a [Object],
    new_bucket_items: &'a [Object],
) -> HashSet<String> {
    println!("Converting old items to a HashSet...");
    let au_set: HashSet<_> = old_bucket_items
        .iter()
        .map(|object| ObjectKey::from(object).key)
        .collect();
    println!("Converting new items to a HashSet...");
    let us_set: HashSet<_> = new_bucket_items
        .iter()
        .map(|object| ObjectKey::from(object).key)
        .collect();

    println!("Performing diff...");
    au_set.difference(&us_set).cloned().collect()
}

async fn get_missing_objects(
    client: &Client,
    bucket: &str,
    missing_items: HashSet<String>,
    path: String,
) -> Result<(), Error> {
    let mut tasks = Vec::new();
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_OPERATIONS));

    for key in missing_items {
        let client = client.clone();
        let bucket = bucket.to_string();
        let sema_clone = semaphore.clone();

        // Spawn a new task for each object
        let p = path.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = sema_clone.acquire().await.unwrap();
            process_object(&client, &bucket, &key, p).await
        }));
    }
    join_all(tasks).await;
    Ok(())
}

async fn download_all_objects(
    client: &Client,
    bucket: &str,
    objects: Vec<Object>,
    path: String,
) -> Result<(), Error> {
    let mut tasks = Vec::new();
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_OPERATIONS));

    for object in objects {
        let client = client.clone();
        let bucket = bucket.to_string();
        let sema_clone = semaphore.clone();

        // Spawn a new task for each object
        let p = path.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = sema_clone.acquire().await.unwrap();
            process_object(&client, &bucket, &object.key.unwrap(), p).await
        }));
    }
    join_all(tasks).await;
    Ok(())
}

async fn process_object(
    client: &Client,
    bucket: &str,
    key: &str,
    path: String,
) -> Result<(), Error> {
    let get_obj_resp = client.get_object().bucket(bucket).key(key).send().await?;
    let body = match get_obj_resp.body.collect().await {
        Ok(b) => b,
        Err(e) => {
            println!("Got an error downloading {}: {}", key, e);
            return Ok(());
        }
    };
    let data = body.into_bytes().to_vec();

    let local_path = PathBuf::from(format!("{path}/")).join(&bucket).join(&key);

    // Create the directory if it does not exist
    if let Some(parent) = local_path.parent() {
        if !parent.exists() {
            match fs::create_dir_all(parent).await {
                Ok(d) => d,
                Err(e) => {
                    println!("Got an error create file {}: {}", key, e);
                    return Ok(());
                }
            };
        }
    }

    let mut file = match File::create(&local_path).await {
        Ok(f) => f,
        Err(e) => {
            println!("Got an error create file {}: {}", key, e);
            return Ok(());
        }
    };
    match file.write_all(&data).await {
        Ok(w) => w,
        Err(e) => {
            println!("Got an error writing file {}: {}", key, e);
            return Ok(());
        }
    };

    //println!("Downloaded and saved: {}", key);

    Ok(())
}

async fn upload_missing_objects(client: &Client, bucket: &str, dir: String) -> Result<(), Error> {
    let mut tasks = Vec::new();
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_OPERATIONS));

    let path_pattern = format!("{dir}/**/*");
    let file_paths = match glob(path_pattern.as_str()) {
        Ok(f) => f,
        Err(e) => {
            println!("Error globbing: {}", e);
            return Ok(());
        }
    };

    for local_path in file_paths {
        let path = local_path.unwrap();
        if path.is_dir() {
            continue;
        }
        let client = client.clone();
        let target_bucket = bucket.to_string();
        let sema_clone = semaphore.clone();
        let key = match path.strip_prefix(format!("{}/", dir).as_str()) {
            Ok(k) => k.to_str().unwrap().to_string(),
            Err(e) => {
                println!("Error getting key name from path: {}", e);
                return Ok(());
            }
        };

        tasks.push(tokio::spawn(async move {
            let _permit = sema_clone.acquire().await.unwrap();
            upload_object(&client, &target_bucket, &key, path).await
        }));
    }

    // Wait for all uploads to complete
    join_all(tasks).await;
    Ok(())
}

async fn upload_object(
    client: &Client,
    bucket: &str,
    key: &str,
    local_path: PathBuf,
) -> Result<(), Error> {
    let mut file = File::open(&local_path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(data.into())
        .send()
        .await?;

    println!("Uploaded: {}", key);

    Ok(())
}
