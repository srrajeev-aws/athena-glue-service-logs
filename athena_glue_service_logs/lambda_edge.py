# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed 
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
# express or implied. See the License for the specific language governing 
# permissions and limitations under the License.

"""Implementation for converting and partitioning Lambda@Edge Logs"""
from athena_glue_service_logs.catalog_manager import BaseCatalogManager
from athena_glue_service_logs.partitioners.date_partitioner import DatePartitioner
from athena_glue_service_logs.partitioners.grouped_date_partitioner import GroupedDatePartitioner

class LambdaEdgeRawCatalog(BaseCatalogManager):
    """An implementatin of BaseCatalogManager for Lambda@Edge Logs"""
    
    def get_partitioner(self):
        return GroupedDatePartitioner(s3_location=self.s3_location, hive_compatible=True)

    def timestamp_field(self):
        return "time"
        
    def _build_storage_descriptor(self, partition_values=None):
        if partition_values is None:
            partition_values = []
        return {
            "Columns": [
                {"Name": "executionregion", "Type": "string"},
                {"Name": "requestid", "Type": "string"},
                {"Name": "distributionid", "Type": "string"},
                {"Name": "distributionname", "Type": "string"},
                {"Name": "eventtype", "Type": "string"},
                {"Name": "requestdata", "Type": "double"},
                {"Name": "customtraceid", "Type": "double"},
            ],
            "Location": self.partitioner.build_partitioned_path(partition_values),
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                "Parameters" : {}
            },
            "BucketColumns": [],  # Required or SHOW CREATE TABLE fails
            "Parameters": {}  # Required or create_dynamic_frame.from_catalog fails for partitions
        }
        

class LambdaEdgeConvertedCatalog(BaseCatalogManager):
    """An implementation of BaseCatalogManager for Lambda@Edge converted tables"""

    def get_partitioner(self):
        return GroupedDatePartitioner(s3_location=self.s3_location, hive_compatible=True)

    def timestamp_field(self):
        return "time"

    def _build_storage_descriptor(self, partition_values=None):
        if partition_values is None:
            partition_values = []
        return {
            "Columns": [
                {"Name": "executionregion", "Type": "string"},
                {"Name": "requestid", "Type": "string"},
                {"Name": "distributionid", "Type": "string"},
                {"Name": "distributionname", "Type": "string"},
                {"Name": "eventtype", "Type": "string"},
                {"Name": "requestdata", "Type": "string"},
                {"Name": "customtraceid", "Type": "string"},
            ],
            "Location": self.partitioner.build_partitioned_path(partition_values),
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {}
            },
            "BucketColumns": [],  # Required or SHOW CREATE TABLE fails
            "Parameters": {}  # Required or create_dynamic_frame.from_catalog fails for partitions
        }
