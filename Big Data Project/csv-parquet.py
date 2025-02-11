import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Intialsing spark & GLue Context 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Filtering Data Using predicate_pushdown
predicate_pushdown = "region in ('ca','gb','us')"

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "db-yt-clean", table_name = "raw_statistics", transformation_ctx = "datasource0", push_down_predicate = predicate_pushdown)

# Schema Transformation 
applymapping1 = ApplyMapping.apply(
    frame=datasource0, 
    mappings=[
        ("video_id", "string", "video_id", "string"), 
        ("trending_date", "string", "trending_date", "string"), 
        ("title", "string", "title", "string"), 
        ("channel_title", "string", "channel_title", "string"), 
        ("category_id", "string", "category_id", "bigint"), 
        ("publish_time", "string", "publish_time", "string"), 
        ("tags", "string", "tags", "string"), 
        ("views", "string", "views", "bigint"), 
        ("likes", "string", "likes", "bigint"), 
        ("dislikes", "string", "dislikes", "bigint"), 
        ("comment_count", "string", "comment_count", "bigint"), 
        ("thumbnail_link", "string", "thumbnail_link", "string"), 
        ("comments_disabled", "string", "comments_disabled", "boolean"),  # Fixed
        ("ratings_disabled", "string", "ratings_disabled", "boolean"),  # Fixed
        ("video_error_or_removed", "string", "video_error_or_removed", "boolean"),  # Fixed
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string")  
    ], 
    transformation_ctx="applymapping1"
)

# To ensure data consistancy 
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")

# Drop Null Value 
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")

# Reduces the number of partition to one file per region 
datasink1 = dropnullfields3.toDF().coalesce(1)

# Dynamic Frame => Data Frame : Better Performance 
df_final_output = DynamicFrame.fromDF(datasink1, glueContext, "df_final_output")

datasink4 = glueContext.write_dynamic_frame.from_options(frame=df_final_output, connection_type="s3", format="glueparquet", connection_options={"path": "s3://yt-cleaned-stockholm-eu-dev/raw_statistics/", "partitionKeys": ["region"]}, format_options={"compression": "snappy"}, transformation_ctx="datasink4")

job.commit()
