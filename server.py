import uvicorn
from fastapi import FastAPI

app = FastAPI(debug=True)

@app.get("/")

async def index():
    return {"Hello World!"}



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)