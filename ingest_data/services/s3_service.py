import boto3
from io import BytesIO
from pandas import DataFrame
from datetime import datetime

from common.logger import log, auto_log

class S3Service:
    def __init__(self, aws_access_key_id : str, aws_secret_access_key : str, endpoint_url : str = "http://127.0.0.1:9000"):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url,
            config=boto3.session.Config(signature_version='s3v4'),
            aws_session_token=None
        )

    def __full_filename(self, *args) -> str:
        return "/".join(*args)

    @auto_log()
    def put_data(self, data : BytesIO) -> None:
        now = datetime.now()
        year = str(now.year)
        month = str(now.month).zfill(2)
        day = str(now.day).zfill(2)
        key = self.__full_filename(["pos", year, month, day, "sales.xlsx"])
        self.client.put_object(Bucket="files", Key=key, Body=data.getvalue())
        log.info(f"Put {key} was successfully")