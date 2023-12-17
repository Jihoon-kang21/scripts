# Copyright (c) 2022, Gauss Labs, Inc. All rights reserved.
#
# Unauthorized copying of this file is strictly prohibited.
# Proprietary and confidential.

"""ticket proxy for helpdesk"""

import datetime
import json
import logging
from typing import List, Union
import httpx
import fastapi
from fastapi.requests import Request
from fastapi.responses import JSONResponse
import psycopg2

conn = psycopg2.connect(
    database="monitoring",
    user="postgres",
    password="postgres",
    host="postgresserveraddress",
    port="5000",
    connect_timeout=5
)
cursor = conn.cursor()

cursor.execute(
    '''CREATE TABLE IF NOT EXISTS ticket_mapping (
    ticket_id TEXT PRIMARY KEY,
    request_id TEXT,
    timestamp TIMESTAMPTZ
    )'''
)
conn.commit()

now = datetime.datetime.now()
today = now.strftime('%Y-%m-%d')

expected_date_raw = now + datetime.timedelta(days=7)
expected_date = expected_date_raw.strftime('%Y-%m-%d')

app = fastapi.FastAPI()
_logger = logging.getLogger("uvicorn.panoptes")
_logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s][%(levelname)s|%(filename)s:%(lineno)s] >> %(message)s')

streamHandler = logging.StreamHandler()
streamHandler.setFormatter(formatter)

fileMaxByte = 1024 * 1024 * 5
fileHandler = logging.handlers.RotatingFileHandler('./helpdesk-hook.log', maxBytes=fileMaxByte, backupCount=20)
fileHandler.setFormatter(formatter)

itsmurl = "http://itsm.com/api"

def itsm_header() -> dict:
    res = dict()
    res["Authorization"] = "Bacic c25wqkasd093-base64token"
    return res


def itsm_newticket(writer: str, category: str, subject: str, contents: str) -> dict:
    res = dict()

    res["code"] = "000"
    res["item_id"] = "카탈로그 아이템 번호"
    res["service"] = "BSN0003205"
    res["req_user_id"] = writer
    res["reg_user_id"] = writer
    res["request_purpose"] = category
    res["priority"] = 2
    res["end_hop_date"] = expected_date
    res["title"] = subject
    res["contents"] = contents
    res["after_improvement"] = "없음"
    return res

def itsm_assign(ticket_id: str, technician: str) -> dict:
    
    cursor.execute("SELECT request_id FROM ticket_mapping WHERE ticket_id= ?", (ticket_id,))
    request_id_tuple = cursor.fetchone()
    request_id = request_id_tuple[0]
    
    res = dict()
    res["code"] = "020"
    res["request_id"] = request_id
    res["rcv_user_id"] = technician
    res["trt_user_id"] = technician
    res["end_expect_date"] = expected_date
    res["comment"] = "요청이 접수되었습니다."
    return res

def itsm_complete(ticket_id: str, date: str) -> dict:
    
    cursor.execute("SELECT request_id FROM ticket_mapping WHERE ticket_id= ?", (ticket_id,))
    result_id_tuple = cursor.fetchone()
    request_id = result_id_tuple[0]

    open = datetime.datetime.strptime(date, "%m%d%Y %I:%N:%S %p")
    timeDifference = now - open
    hoursFloat = timeDifference.total_seconds() / 3600
    hoursInt = int(hoursFloat)
    hours = str(hoursInt)

    res = dict()
    res["code"] = "030"
    res["request_id"] = request_id
    res["work_qty"] = hours
    res["comment"] = "처리가 완료되었습니다."
    return res

@app.post("/itsm_new")
async def itsm_new(req: Request):
    try:
        hd_data = await req.json()
        
    except Exception as e:
        _logger.exception("Fetching a request failed:")
        return JSONResponse(str(e), status_code=401)

    _logger.info("API call for /itsm_new: Incoming=%s", hd_data)
    ticket_id = hd_data["ticket_id"]
    writer = hd_data["writer"]
    category = hd_data["category"]
    subject = hd_data["subject"]
    contents = hd_data["contents"]
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        itsm_headers = itsm_header()
        itsm_body = itsm_newticket(writer, category, subject, contents)
        _logger.debug("Try to send ITSM new request: url=%s, header=%s, body=%s", itsmurl, itsm_headers, itsm_body)
        res = await client.post(itsmurl, headers=itsm_header(), json=itsm_body)
        if res.is_client_error:
            _logger.error("Failed to send ITSM new ticket API request due to client error: %s", res.text)
            return {}
        elif res.is_server_error:
            _logger.warning("Failed to send ITSM new ticket API request due to server error: %s", res.text)
            return {}
        else:
            _logger.info("ITSM new ticket API request is sent successfully.")
            response_data = res.json()
            _logger.info(f"Response: {response_data}")
            request_id = response_data.get("result", {}).get("request_id", "")
            _logger.info(f"Request ID: {request_id}")

            cursor.execute('''
                INSERT OR REPLACE INTO ticket_mapping (ticket_id, request_id, timestamp) VALUES (%s, %s, %s)
            ''', (ticket_id, request_id, now))
            conn.commit()
            
    return JSONResponse(hd_data)


@app.post("/itsm_ass")
async def itsm_ass(req: Request):
    try:
        hd_data = await req.json()
        
    except Exception as e:
        _logger.exception("Fetching a request failed:")
        _logger.error(f"hd_data: {req}")
        _logger.error(f"Error in itsm_ass: {str(e)}")
        return JSONResponse(str(e), status_code=401)

    _logger.info("API call for /itsm_ass: incoming=%s", hd_data)
    ticket_id = hd_data["ticket_id"]
    technician = hd_data["technician"]
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        itsm_headers = itsm_header()
        itsm_body = itsm_assign(ticket_id, technician)
        _logger.debug("Try to send ITSM assign request: url=%s, header=%s, body=%s", itsmurl, itsm_headers, itsm_body)

        res = await client.post(itsmurl, headers=itsm_headers, json=itsm_body)
        if res.is_client_error:
            _logger.error("Failed to send ITSM assign API request due to client error: %s", res.text)
            return {}
        elif res.is_server_error:
            _logger.warning("Failed to send ITSM assign API request due to server error: %s", res.text)
            return {}
        else:
            _logger.info("ITSM Assigning Technician API request is sent successfully.")
            response_data = res.json()
            _logger.info(f"Response: {response_data}")
            
    return JSONResponse(hd_data)


@app.post("/itsm_com")
async def itsm_com(req: Request):
    try:
        hd_data = await req.json()
        
    except Exception as e:
        _logger.exception("Fetching a request failed:")
        return JSONResponse(str(e), status_code=401)
    _logger.debug("ITSM Complete Issue API call for /itsm_com: incoming=%s", hd_data)

    ticket_id = hd_data["ticket_id"]
    date = hd_data["data"]
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        itsm_headers = itsm_header()
        itsm_body = itsm_complete(ticket_id,date)
        _logger.debug("Try to send ITSM complete request: url=%s, header=%s, body=%s", itsmurl, itsm_headers, itsm_body)
        res = await client.post(itsmurl, headers=itsm_headers, json=itsm_body)
        if res.is_client_error:
            _logger.error("Failed to send ITSM complete API request due to client error: %s", res.text)
            return {}
        elif res.is_server_error:
            _logger.warning("Failed to send ITSM complete API request due to server error: %s", res.text)
            return {}
        else:
            _logger.info("ITSM Complete Issue API request is sent successfully")
            response_data = res.json()
            _logger.info(f"Response: {response_data}")
            
    return JSONResponse(hd_data)

@app.on_event("startup")
async def start_up():
    _logger.setLevel(logging.DEBUG)
