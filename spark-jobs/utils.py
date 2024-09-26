import json
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

# Spark df Schema -> Avro Schema
def get_avro_schema(spark_df, namespace:str, schema_type:str, name:str):

    schema_base = {
    "namespace": namespace,
    "type": schema_type,
    "name": name
    }

    avro_mapping = {
        'BooleanType()' : ["boolean", "null"],
        'IntegerType()' :  ["int", "null"],
        'LongType()' : ["long", "null"],
        'FloatType()' : ["float", "null"],
        'DoubleType()': ["double", "null"],
        'StringType()' : ["string", "null"],
        'TimestampType()' : {"type": "long", "logicalType": "timestamp-millis"},
        'ArrayType(StringType,true)' : [{"type": "array", "items": ["string", "null"]}, "null"],
        'ArrayType(IntegerType,true)' : [{"type": "array", "items": ["int", "null"]}, "null"],
        "StructType([StructField('start', TimestampType(), True), StructField('end', TimestampType(), True)])" : 
            [{
                "type": "record", "name": "window", "fields": 
                [{"name": "start", "type": "long", "logicalType": "timestamp-millis"},
                {"name": "end", "type": "long", "logicalType": "timestamp-millis"}]
            }]

        }
    
    fields = []

    for field in spark_df.schema.fields:
        if (str(field.dataType) in avro_mapping):
            fields.append({"name" : field.name, "type": avro_mapping[str(field.dataType)]})
        else:
            fields.append({"name" : field.name, "type": str(field.dataType)})

    schema_base["fields"] = fields
    return json.dumps(schema_base)

def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)
    return sr, latest_version

def register_schema(schema_registry_url, schema_registry_subject, schema_str):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    schema = Schema(schema_str, schema_type="AVRO")
    schema_id = sr.register_schema(subject_name=schema_registry_subject, schema=schema)
    return schema_id