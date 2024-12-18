import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1734487179525 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://data-health/silver/run-1734485938737-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1734487179525")

# Script generated for node Amazon S3
AmazonS3_node1734488996473 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://data-health/silver/run-1734485959950-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1734488996473")

# Script generated for node Amazon S3
AmazonS3_node1734488729122 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://data-health/silver/run-1734485959116-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1734488729122")

# Script generated for node Amazon S3
AmazonS3_node1734487382212 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://data-health/silver/run-1734485944254-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1734487382212")

# Script generated for node Amazon S3
AmazonS3_node1734487726792 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://data-health/silver/run-1734485951562-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1734487726792")

# Script generated for node Join
AmazonS3_node1734487382212DF = AmazonS3_node1734487382212.toDF()
AmazonS3_node1734488996473DF = AmazonS3_node1734488996473.toDF()
Join_node1734489396737 = DynamicFrame.fromDF(AmazonS3_node1734487382212DF.join(AmazonS3_node1734488996473DF, (AmazonS3_node1734487382212DF['faixa-etaria'] == AmazonS3_node1734488996473DF['faixa_etaria']), "outer"), glueContext, "Join_node1734489396737")

# Script generated for node Join
AmazonS3_node1734487179525DF = AmazonS3_node1734487179525.toDF()
AmazonS3_node1734487726792DF = AmazonS3_node1734487726792.toDF()
Join_node1734489312043 = DynamicFrame.fromDF(AmazonS3_node1734487179525DF.join(AmazonS3_node1734487726792DF, (AmazonS3_node1734487179525DF['ano'] == AmazonS3_node1734487726792DF['ano']), "outer"), glueContext, "Join_node1734489312043")

# Script generated for node Join
Join_node1734489312043DF = Join_node1734489312043.toDF()
AmazonS3_node1734488729122DF = AmazonS3_node1734488729122.toDF()
Join_node1734489618379 = DynamicFrame.fromDF(Join_node1734489312043DF.join(AmazonS3_node1734488729122DF, (Join_node1734489312043DF['ano'] == AmazonS3_node1734488729122DF['ano']), "outer"), glueContext, "Join_node1734489618379")

# Script generated for node Amazon S3 - taxa etaria
EvaluateDataQuality().process_rows(frame=Join_node1734489396737, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734483279481", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3taxaetaria_node1734489718902 = glueContext.write_dynamic_frame.from_options(frame=Join_node1734489396737, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-health/gold/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3taxaetaria_node1734489718902")

# Script generated for node Amazon S3 - taxa ano
EvaluateDataQuality().process_rows(frame=Join_node1734489618379, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734483279481", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3taxaano_node1734489659311 = glueContext.write_dynamic_frame.from_options(frame=Join_node1734489618379, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-health/gold/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3taxaano_node1734489659311")

job.commit()
