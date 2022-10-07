from fastapi import FastAPI
from typing import List
import databases
import sqlalchemy
from pydantic import BaseModel

DATABASE_URL = "postgresql://glsmgmsmdngzvf:26dc1f9f0320b0c724d44ca50ff7d2c745bbe55e268c80eea0d83ccb620c31c3@ec2-3-93-206-109.compute-1.amazonaws.com:5432/dek8qtfv9rl3f"

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
