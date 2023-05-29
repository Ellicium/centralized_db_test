import logging
from fastapi import FastAPI, responses, Response, HTTPException, Depends, status
from fastapi.logger import logger
from sqlalchemy.orm import Session
from typing import Annotated, Union

import models
from database import engine, SessionLocal, get_db



# logging
gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)

app = FastAPI(debug=True)



@app.get("/sqlalchemy")
def test_conn(db: Session = Depends(get_db)):
    suppliers = db.query(models.DimSupplier).limit(12).all()
    print(suppliers)
    logger.info("connection success",suppliers)
    return {"data":suppliers}

































# from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

# from msal import ConfidentialClientApplication
# TENANT_ID = "<Your-Azure-Tenant-ID>"
# CLIENT_ID = "<Your-Client-ID>"
# CLIENT_SECRET = "<Your-Client-Secret>"

# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token")

# CLIENT_ID = "c0a70949-359e-4c3d-bfca-082ae51b45e6"
# CLIENT_SECRET = "IHT8Q~2NOl3ww9EkKiO9OxDmJA4r4HIQHC4sQdcL"
# TENANT_ID = "756e24a1-ea84-4a3f-a500-10aea6da0d65"
# AUTHORITY = "https://login.microsoftonline.com/756e24a1-ea84-4a3f-a500-10aea6da0d65"
# SCOPE = ["User.Read"]  # Example scope, adjust as per your requirements

# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token")

# appmsal = ConfidentialClientApplication(
#     client_id=CLIENT_ID,
#     client_credential=CLIENT_SECRET,
#     authority=AUTHORITY
# )

# async def authenticate_user(username: str, password: str):
#     try:
#         result = await appmsal.acquire_token_by_username_password(
#             username=username,
#             password=password,
#             scopes=SCOPE
#         )
#         return result
#     except Exception as e:
#         logger.error(exc_info=e)

# @app.get("/protected_route")
# async def protected_route(token: str = Depends(oauth2_scheme)):
#     try:
#         # Access token is available in the `token` variable
#         # Perform necessary operations
#         return {"message": "Authenticated route"}
#     except Exception as e:
#         logger.error(exc_info=e)


# @app.post("/token")
# async def login(username: str, password: str):
#     try:
#         user_token = await authenticate_user(username, password)
#         return {"access_token": user_token["access_token"], "token_type": "bearer"}
#     except Exception as e:
#         logger.error(exc_info=e)



# @app.get("/items/")
# async def read_items(token: Annotated[str, Depends(oauth2_scheme)]):
#     try:
#         return {"token": token}
#     except Exception as e:
#         logger.error(exc_info=e)