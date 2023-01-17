from google.cloud import bigquery_datatransfer
from google.protobuf import field_mask_pb2

query_string = """ 
WITH article_info AS (
SELECT
INITCAP(title) AS article_title
,ups
,num_comments
,domain
FROM `ornate-reef-332816.reddit_news.r_news` AS r_news
) SELECT * FROM article_info
"""

with open("FlatConnect.module2_v2-prod.sql") as file:
    query_str = file.read()
query_string = """q""".replace("q", query_str)
print(query_string)

service_account_name = "qa-qc-prod@nih-nci-dceg-connect-prod-6d04.iam.gserviceaccount.com"
transfer_config_name = "projects/155089172944/locations/us/transferConfigs/63a134bd-0000-2c48-8c16-883d24f91d30"
new_display_name = "FlatConnect.module2_v2_JP"

transfer_client = bigquery_datatransfer.DataTransferServiceClient()

transfer_config = bigquery_datatransfer.TransferConfig(name=transfer_config_name)
transfer_config.display_name = new_display_name
transfer_config.params = params={"query": query_string, "write_disposition": "WRITE_TRUNCATE"}

transfer_config = transfer_client.update_transfer_config(
    {
        "transfer_config": transfer_config,
        "update_mask": field_mask_pb2.FieldMask(paths=["display_name"]),
    }
)

print(f"Updated config: '{transfer_config.name}'")
print(f"New display name: '{transfer_config.display_name}'")