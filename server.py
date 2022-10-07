from fastapi import FastAPI
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

notes = sqlalchemy.Table(
    "notes",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("text", sqlalchemy.String),
    sqlalchemy.Column("completed", sqlalchemy.Boolean),
)
engine = sqlalchemy.create_engine(
    DATABASE_URL
)
metadata.create_all(engine)


class Notes(BaseModel):
    id: int
    text: str
    completed: bool


class NotesIn(BaseModel):
    text: str
    completed: bool


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/notes/", response_model=List[Notes])
async def read_notes():
    query = notes.select()
    return await database.fetch_all(query)


@app.post("/notes/", response_model=Notes)
async def create_note(note: NotesIn):
    query = notes.insert().values(text=note.text, completed=note.completed)
    last_record_id = await database.execute(query)
    return {**note.dict(), "id": last_record_id}


@app.get("/")
async def index():
    return {"Message": "Success"}
