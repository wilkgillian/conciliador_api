from datetime import datetime
import json
import openpyxl
from fastapi import FastAPI, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import databases
import pandas as pd
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
    file_url: str
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
    query = "SELECT file_url FROM files ORDER BY upload_at DESC LIMIT 5"
    await database.execute(query)
    var = await database.fetch_all(query)
    vendas_cielo_url = str(tuple(var[4].values())).replace("('", "")
    vendas_cielo_url_replace = vendas_cielo_url.replace("',)", "")
    vendas_sig_url = str(tuple(var[3].values())).replace("('", "")
    vendas_sig_url_replace = vendas_sig_url.replace("',)", "")
    recebimentos_cielo_url = str(tuple(var[2].values())).replace("('", "")
    recebimentos_cielo_url_replace = recebimentos_cielo_url.replace("',)", "")
    recebimentos_sig_url = str(tuple(var[1].values())).replace("('", "")
    recebimentos_sig_url_replace = recebimentos_sig_url.replace("',)", "")
    mxm_url = str(tuple(var[0].values())).replace("('", "")
    mxm_url_replace = mxm_url.replace("',)", "")

    df_vendas_cielo = pd.read_excel(vendas_cielo_url_replace, usecols=[
        "Código de autorização", "Valor da venda"])
    df_vendas_sig = pd.read_excel(vendas_sig_url_replace, usecols=[
        "Aut. de Venda", "Valor Proporcional"])
    df_recebimentos_cielo = pd.read_excel(recebimentos_cielo_url_replace, usecols=[
        "Data de pagamento", "Código de autorização", "Valor bruto"])
    df_recebimentos_sig = pd.read_excel(recebimentos_sig_url_replace, usecols=[
        "Aut. de Venda", "Valor Proporcional"])
    df_mxm = pd.read_excel(mxm_url_replace, usecols=[
                           'Data', 'Histórico', 'Débito', 'Crédito'], skipfooter=1)

    grup_vendas_cielo = df_vendas_cielo.groupby(
        pd.Grouper(key='Código de autorização')).sum()
    grup_vendas_sig = df_vendas_sig.groupby(
        pd.Grouper(key='Aut. de Venda')).sum()
    grup_recebimentos_cielo = df_recebimentos_cielo.groupby(
        pd.Grouper(key='Código de autorização')).sum()
    grup_recebimentos_sig = df_recebimentos_sig.groupby(
        pd.Grouper(key='Aut. de Venda')).sum()
    grup_recebimentos_cielo2 = df_recebimentos_cielo.groupby(
        pd.Grouper(key='Data de pagamento')).sum()
    grup_mxm = df_mxm.groupby(pd.Grouper(key="Data")).sum()

    vendas_cieloXsig = pd.merge(pd.DataFrame(
        grup_vendas_cielo), pd.DataFrame(grup_vendas_sig), left_on="Código de autorização", right_on="Aut. de Venda", right_index=True)

    recebimentos_cieloXsig = pd.merge(pd.DataFrame(grup_recebimentos_cielo), pd.DataFrame(
        grup_recebimentos_sig), left_on="Código de autorização", right_on="Aut. de Venda", right_index=True)

    razao_contabilXcielo = pd.merge(pd.DataFrame(grup_recebimentos_cielo2), pd.DataFrame(
        grup_mxm), left_on="Data de pagamento", right_on="Data", right_index=True)

    wb = openpyxl.Workbook()
    wb.create_sheet('diferencas_vendas_cieloxsig')
    diferencas_vendas_cieloxsig = wb['diferencas_vendas_cieloxsig']
    diferencas_vendas_cieloxsig.append(
        ['id', 'aut_pagamento', 'valor_cielo', 'valor_sig', 'diferenca'])

    for index, row in vendas_cieloXsig.iterrows():
        v_cielo = row["Valor da venda"]
        v_sig = row["Valor Proporcional"]
        if round(v_cielo) != round(v_sig):
            diferenca = round(v_cielo, 2) - round(v_sig, 2)
            diferencas_vendas_cieloxsig.append([str(
                index), str(
                index), v_cielo, v_sig, str(round(diferenca, 2)).replace("-", "")])

    wb.create_sheet('diferencas_recebimentos_cieloxsig')
    diferencas_recebimentos_cieloxsig = wb['diferencas_recebimentos_cieloxsig']
    diferencas_recebimentos_cieloxsig.append(
        ['id', 'aut_pagamento', 'valor_cielo', 'valor_sig', 'diferenca'])

    for index, row in recebimentos_cieloXsig.iterrows():
        r_cielo = row["Valor bruto"]
        r_sig = row["Valor Proporcional"]
        if round(r_cielo) != round(r_sig):
            diferenca = round(r_cielo, 2) - round(r_sig, 2)
            diferencas_recebimentos_cieloxsig.append([str(
                index), str(
                index), r_cielo, r_sig, str(round(diferenca, 2)).replace("-", "")])

    wb.create_sheet('diferencas_mxm_cielo')
    diferenca_cieloxmxm = wb['diferencas_mxm_cielo']
    diferenca_cieloxmxm.append(
        ['id', 'data_recebimento', 'valor_cielo', 'valor_mxm', 'diferenca'])

    for index, row in razao_contabilXcielo.iterrows():
        v_mxm = row["Crédito"]
        v_cielo_m = row["Valor bruto"]
        if round(v_cielo_m) != round(v_mxm):
            diferenca = round(v_cielo_m, 2) - round(v_mxm, 2)
            diferenca_cieloxmxm.append([str(index), str(
                index), v_cielo_m, v_mxm, str(round(diferenca, 2)).replace("-", "")])

    wb.save(filename="teste.xlsx")
    dif_vendas = pd.read_excel(
        "teste.xlsx", "diferencas_vendas_cieloxsig", dtype="string")
    dif_recebimentos = pd.read_excel(
        "teste.xlsx", "diferencas_recebimentos_cieloxsig", dtype="string")
    dif_razao_contabil = pd.read_excel(
        "teste.xlsx", "diferencas_mxm_cielo", dtype="string")

    d_v = {"dif_vendas_cielo_sig": [dif_vendas], "dif_recebimentos_cielo_sig": [
        dif_recebimentos], "dif_razao_contabil": [dif_razao_contabil]}
    json_dataframe = pd.DataFrame(d_v).to_json(orient="records")

    json_obj = str(json_dataframe).replace("\\", "")

    return json.loads(json_obj)
