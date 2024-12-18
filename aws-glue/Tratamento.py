import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Amazon S3 - taxamortalidademama
AmazonS3taxamortalidademama_node1734484275550 = glueContext.create_dynamic_frame.from_catalog(database="data-health", table_name="taxamortalidademama_csv", transformation_ctx="AmazonS3taxamortalidademama_node1734484275550")

# Script generated for node Amazon S3 - obitosfaixaetaria
AmazonS3obitosfaixaetaria_node1734483593142 = glueContext.create_dynamic_frame.from_catalog(database="data-health", table_name="obitosporfaixaetaria_csv", transformation_ctx="AmazonS3obitosfaixaetaria_node1734483593142")

# Script generated for node Amazon S3 - taxamortalidadegeral
AmazonS3taxamortalidadegeral_node1734484217155 = glueContext.create_dynamic_frame.from_catalog(database="data-health", table_name="taxamortalidadegeral_csv", transformation_ctx="AmazonS3taxamortalidadegeral_node1734484217155")

# Script generated for node Amazon S3 - mortesanobrasil
AmazonS3mortesanobrasil_node1734483304012 = glueContext.create_dynamic_frame.from_catalog(database="data-health", table_name="mortesanobrasil_csv", transformation_ctx="AmazonS3mortesanobrasil_node1734483304012")

# Script generated for node Amazon S3 - taxamortalidaddeestado
AmazonS3taxamortalidaddeestado_node1734484112160 = glueContext.create_dynamic_frame.from_catalog(database="data-health", table_name="taxamortalidadeestado_csv", transformation_ctx="AmazonS3taxamortalidaddeestado_node1734484112160")

# Script generated for node Amazon S3 - taxamundialxbrasil
AmazonS3taxamundialxbrasil_node1734484348104 = glueContext.create_dynamic_frame.from_catalog(database="data-health", table_name="taxamundialxbrasil_csv", transformation_ctx="AmazonS3taxamundialxbrasil_node1734484348104")

# Script generated for node SQL Query
SqlQuery232 = '''
select * from myDataSource

'''
SQLQuery_node1734484291836 = sparkSqlQuery(glueContext, query = SqlQuery232, mapping = {"myDataSource":AmazonS3taxamortalidademama_node1734484275550}, transformation_ctx = "SQLQuery_node1734484291836")

# Script generated for node SQL Query
SqlQuery229 = '''
select * from myDataSource

'''
SQLQuery_node1734483642583 = sparkSqlQuery(glueContext, query = SqlQuery229, mapping = {"myDataSource":AmazonS3obitosfaixaetaria_node1734483593142}, transformation_ctx = "SQLQuery_node1734483642583")

# Script generated for node SQL Query
SqlQuery230 = '''
select * from myDataSource

'''
SQLQuery_node1734484248843 = sparkSqlQuery(glueContext, query = SqlQuery230, mapping = {"myDataSource":AmazonS3taxamortalidadegeral_node1734484217155}, transformation_ctx = "SQLQuery_node1734484248843")

# Script generated for node SQL Query
SqlQuery227 = '''
select * from myDataSource

'''
SQLQuery_node1734483404922 = sparkSqlQuery(glueContext, query = SqlQuery227, mapping = {"myDataSource":AmazonS3mortesanobrasil_node1734483304012}, transformation_ctx = "SQLQuery_node1734483404922")

# Script generated for node SQL Query
SqlQuery231 = '''
select * from myDataSource

'''
SQLQuery_node1734484164188 = sparkSqlQuery(glueContext, query = SqlQuery231, mapping = {"myDataSource":AmazonS3taxamortalidaddeestado_node1734484112160}, transformation_ctx = "SQLQuery_node1734484164188")

# Script generated for node SQL Query
SqlQuery228 = '''
select * from myDataSource

'''
SQLQuery_node1734484376389 = sparkSqlQuery(glueContext, query = SqlQuery228, mapping = {"myDataSource":AmazonS3taxamundialxbrasil_node1734484348104}, transformation_ctx = "SQLQuery_node1734484376389")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734484291836, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734483279481", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1734484319486 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1734484291836, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-health/silver/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1734484319486")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734483642583, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734483279481", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1734483662806 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1734483642583, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-health/silver/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1734483662806")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734484248843, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734483279481", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1734484253085 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1734484248843, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-health/silver/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1734484253085")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734483404922, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734483279481", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1734483449240 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1734483404922, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-health/silver/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1734483449240")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734484164188, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734483279481", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1734484175093 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1734484164188, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-health/silver/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1734484175093")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734484376389, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734483279481", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1734484405750 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1734484376389, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-health/silver/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1734484405750")

job.commit()
