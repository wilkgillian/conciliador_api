from datetime import datetime
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import databases
import psycopg2
import sqlalchemy
from sqlalchemy_utils import URLType
from pydantic import BaseModel
import os
import boto3
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = (os.environ["DATABASE"])
bucket_name = (os.environ["AWS_BUCKET_NAME"])

s3 = boto3.client(
    service_name="s3",
    aws_access_key_id=(os.environ["AWS_ACCESS_KEY_ID"]),
    aws_secret_access_key=(os.environ["AWS_SECRET_ACCESS_KEY"]),
    region_name='sa-east-1'
)

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

files = sqlalchemy.Table(
    "files",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String, nullable=False,),
    sqlalchemy.Column("file_url", URLType),
    sqlalchemy.Column("upload_at", sqlalchemy.String,
                      nullable=False, default=datetime.now())
)
engine = sqlalchemy.create_engine(
    DATABASE_URL
)
metadata.create_all(engine)


class Files(BaseModel):
    id: int
    name: str
    upload_at: str


class FilesIn(BaseModel):
    name: str
    upload_at: str


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/files", response_model=List)
async def read_files():
    query = files.select().order_by("id")
    return await database.fetch_all(query)


@app.post("/file/upload")
async def upload_file(arquivo: UploadFile):
    upload_at = datetime.now()

    # bucket = s3. Bucket(bucket_name)
    s3.upload_fileobj(arquivo.file, bucket_name, arquivo.filename)

    uploaded_file_url = f"https://{bucket_name}.s3.amazonaws.com/{str(arquivo.filename)}"

    query = files.insert().values(name=str(arquivo.filename),
                                  file_url=uploaded_file_url, upload_at=str(upload_at))

    last_record_id = await database.execute(query)

    return {"name": arquivo.filename, "url_file": uploaded_file_url, "id": last_record_id, "upload_at": upload_at}


@app.put("/file/{file_id}")
async def update_file(file_id: int, arquivo: UploadFile):
    upload_at = datetime.now()

    s3.upload_fileobj(arquivo.file, bucket_name, arquivo.filename)

    uploaded_file_url = f"https://{bucket_name}.s3.amazonaws.com/{str(arquivo.filename)}"
    query = files.update().where(files.columns.id == file_id).values(
        name=arquivo.filename, file_url=uploaded_file_url, upload_at=str(upload_at))
    await database.execute(query)
    return {"id": file_id, "name": arquivo.filename, "file_url": uploaded_file_url, "upload_at": str(upload_at)}


@app.delete("/file/delete/{file_id}")
async def update_file(file_id: int):
    query = files.delete().where(files.columns.id == file_id)
    await database.execute(query)
    return {"message": "the file with id={} deleted successfully".format(file_id)}


@app.get("/file/conciliado")
async def conciliado():
    query = "SELECT * FROM files ORDER BY upload_at DESC LIMIT 5"
    await database.execute(query)
    return await database.fetch_all(query)
