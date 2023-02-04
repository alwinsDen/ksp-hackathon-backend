from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import sqlalchemy as sa
app = FastAPI()

origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
def say_hello(name: str):
    return {"message": f"Hello {name}"}

@app.post("/search/{key}")
def search(key: str):
    alchemy_engine = create_engine('postgresql+psycopg2://postgres:123@localhost:5432/ksp_hackathon')
    db_connection = alchemy_engine.connect()
    dataFrame = pd.read_sql_table("cleaned_KSP_hackathondata", db_connection)
    all_df_set = list(dataFrame.columns.values.tolist())
    search = key
    datatype_complete = {
        'State':"string",
        'District_Name':"string",
        'PS_Name':"string",
        'FIRNo':"string",
        'FIR_Date':"date",
        'Person_No':"string",
        'Arrest_Date':"date",
        'Person_Name':"string",
        'Father_Name':"string",
        'Gender':"string",
        'AgeWhileOpening':"range",
        'Age': "range",
        'Pres_Address1':"string",
        'Perm_Address1':"string",
        'PersonStatus':"string",
        'Name':"string",
        'Major_Head':"string",
        'Minor_Head':"string",
        'Crime_No':"string",
        'Arr_ID':"string",
        'Unit_ID':"string",
        'fir_id':"string",
        'dedt':"string"
    }
    filter_vals = None
    for index,vls in enumerate(dict(dataFrame.dtypes)):
        if index==0:
            if dataFrame.dtypes[vls]==int or dataFrame.dtypes[vls]==float:
                if search.isnumeric():
                    filter_vals = (dataFrame[vls]==int(search))
                continue
            filter_vals = (dataFrame[vls].str.contains(search, case=False))
            continue
        if dataFrame.dtypes[vls]==int or dataFrame.dtypes[vls]==float:
            if search.isnumeric():
                filter_vals |= (dataFrame[vls]==int(search))
            continue
        print(dataFrame[vls].dtypes)
        filter_vals |= (dataFrame[vls].str.contains(search, case=False))
    filtered_val_reply = []
    for idx_tab in range(0,len(dataFrame[filter_vals].index)):
        to_apned_val = dict(dataFrame[filter_vals].iloc[idx_tab].to_dict())
        to_apned_val["id"] = idx_tab
        filtered_val_reply.append(to_apned_val)
    return {
        "message": filtered_val_reply
    }
