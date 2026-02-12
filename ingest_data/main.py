import os
import pandas as pd
from dotenv import load_dotenv
from io import BytesIO

from services.google_sheet_service import GGSheetService
from services.s3_service import S3Service
from common.logger import log

load_dotenv()

def main():
    SPREAD_SHEET_ID = os.getenv("SPREAD_SHEET_ID")
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    CREDENTIAL_FILE = os.getenv("CREDENTIAL_FILE")
    TOKEN_FILE = os.getenv("TOKEN_FILE")

    log.info("Getting data from Google Sheet")
    sheet_service = GGSheetService()
    sheet_resource = sheet_service.login(TOKEN_FILE, CREDENTIAL_FILE, SCOPES)
    sheet = sheet_service.read_sheet(sheet_resource, SPREAD_SHEET_ID, "Sales")

    sheet_df = pd.DataFrame(sheet.get("values")[1:], columns=sheet.get("values")[0])
    xlsx_buffer = BytesIO()
    sheet_df.to_excel(xlsx_buffer, sheet_name="sales", index=False)

    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
    s3_service = S3Service(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    s3_service.put_data(xlsx_buffer)


if __name__ == "__main__":
    main()