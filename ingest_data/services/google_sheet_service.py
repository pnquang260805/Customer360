# System
import os
import json
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv
from typing import *

# Google
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials

from dataclasses import dataclass

from common.logger import log, auto_log

@dataclass
class GGSheetService:
    @auto_log()
    def login(self, token_file: str, credential_file : str, scopes: List[str]) -> Resource:
        credential = None
        if os.path.exists(token_file):
            credential = Credentials.from_authorized_user_file(token_file, scopes)

        # If there are no (valid) credentials available, let the user log in.
        if not credential or not credential.valid:
            if credential and credential.expired and credential.refresh_token:
                credential.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credential_file, scopes
            )
            credential = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(token_file, "w") as token:
                token.write(credential.to_json())
        log.info("Login successfully")
        service = build("sheets", "v4", credentials=credential)
        return service

    @auto_log()
    def read_sheet(self, service: Resource, spread_sheet_id : str, range: str) -> Dict[str, Any]:
        sheet = service.spreadsheets()
        sheet_data = (
            sheet.values()
            .get(spreadsheetId=spread_sheet_id, range=range)
            .execute()
        )
        return sheet_data
    
    @auto_log()
    def ingest(self, data : Dict[str, Any]):
        pass