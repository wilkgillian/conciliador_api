from datetime import datetime
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import databases
import sqlalchemy
from pydantic import BaseModel
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = (os.environ["DATABASE"])

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

files = sqlalchemy.Table(
    "files",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String, nullable=False,),
    sqlalchemy.Column("size", sqlalchemy.Integer, nullable=False,),
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
    query = files.select()
    return await database.fetch_all(query)


@app.post("/file/upload")
async def upload_file(arquivo: UploadFile = File(...)):
    upload_at = datetime.now()
    query = files.insert().values(name=arquivo.filename, upload_at=str(upload_at))
    last_record_id = await database.execute(query)
    return {"file": arquivo.filename, "id": last_record_id, "upload_at": upload_at}


@app.put("/file/{file_id}")
async def update_file(file_id: int, arquivo: UploadFile = File(...)):
    upload_at = datetime.now()
    query = files.update().where(files.columns.id == file_id).values(
        name=arquivo.filename, upload_at=str(upload_at))
    await database.execute(query)
    return {"file": arquivo.filename, "id": file_id, "upload_at": str(upload_at)}


@app.delete("/file/delete/{file_id}")
async def update_file(file_id: int):
    query = files.delete().where(files.columns.id == file_id)
    await database.execute(query)
    return {"message": "the file with id={} deleted successfully".format(file_id)}
