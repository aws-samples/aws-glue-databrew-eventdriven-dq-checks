import boto3
from crhelper import CfnResource

s3 = boto3.resource("s3")
helper = CfnResource(log_level="DEBUG")


def copy_votes(source_bucket, input_bucket, key):
    copy_source = {"Bucket": source_bucket, "Key": key}
    s3.meta.client.copy(copy_source, input_bucket, key)


@helper.update
def no_op(_, __):
    pass


@helper.create
def create(event, _):
    cfn_input = event["ResourceProperties"]
    if "copyVotes" in cfn_input.keys():
        copy_votes(
            cfn_input["SourceBucketName"],
            cfn_input["InputBucketName"],
            cfn_input["SourceKey"],
        )
    else:
        pass


@helper.delete
def delete(event, _):
    cfn_input = event["ResourceProperties"]
    if "copyVotes" in cfn_input.keys():
        pass

    def empty_bucket(bucket_name):
        bucket = s3.Bucket(bucket_name)
        bucket.objects.all().delete()
        bucket.object_versions.delete()

    for bucket in [cfn_input["InputBucketName"], cfn_input["OutputBucketName"]]:
        empty_bucket(bucket)


def handler(event, context):
    helper(event, context)
