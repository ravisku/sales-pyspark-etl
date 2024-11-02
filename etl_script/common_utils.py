import boto3


def trigger_glue_crawler(crawler_name):
    client = boto3.client("glue", region_name="your-region")
    try:
        response = client.start_crawler(Name=crawler_name)
        print(f"Triggered Glue Crawler: {crawler_name}")
    except client.exceptions.CrawlerRunningException:
        print(f"Crawler {crawler_name} is already running.")
    except Exception as e:
        print(f"Error triggering Glue Crawler: {str(e)}")
