#!/usr/bin/env bash
# Company = Summit Live LLC.
# author  = luisfelipeyepezbarrios | date  =2/13/20
# project = syenapFTP, utils

import ftputil
import pandas as pd
import os
import os.path
import datetime
import time

from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy_utils import database_exists


def get_files(ftp_domain, username, passwd, folder):
    with ftputil.FTPHost(ftp_domain, username, passwd) as ftp_host:
        assert ftp_host.path.isdir(ftp_host.curdir)
        files_list = ftp_host.listdir(folder)
        files_ = []
        for file in files_list:
            if file.endswith(".csv"):
                files_.append(f"{folder}/{file}")
        return files_


def load_file(filename_path):
    with ftputil.FTPHost(
        os.environ["URL_FTP_DOMAIN"],
        os.environ["URL_FTP_USERNAME"],
        os.environ["URL_FTP_PASSW"],
    ) as ftp_host:
        assert ftp_host.path.isdir(ftp_host.curdir)

        with ftp_host.open(filename_path) as file:
            frame = pd.read_csv(file, header=None, sep="\n")
            frame = frame[0].str.split(";", expand=True)

            return frame


def columns_values(frame):
    columns_value = []
    for v in frame.iloc[0, :]:
        v = v.lower().replace(" ", "_")
        columns_value.append(v)
    return columns_value


def transform_file(filename_path):
    frame_test = load_file(filename_path)
    columns_header = columns_values(frame_test)
    frame_ = frame_test.iloc[1:, :]
    frame_.columns = columns_header
    frame_["uuid"] = frame_["storename"] + frame_["date"]
    frame_["reg_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return frame_


def chain_structure(list_ticket_file):
    chain_data = []
    for idx, val in enumerate(list_ticket_file):
        try:
            frame = transform_file(val)
        except Exception as e:
            print(f"Index: {idx} File: {val}")
        else:
            chain_data.append(frame)
        chain_frame = pd.concat(chain_data, ignore_index=True, sort=False)
        return chain_frame


def frame_reg(frame_structure, reg_name):
    outdir = ".temp_reg"
    outname = f"{reg_name}.csv"
    fullname_path = os.path.join(outdir, outname)

    if not os.path.exists(outdir):
        os.mkdir(outdir)
    else:
        pass

    if not os.path.isfile(fullname_path):
        frame_structure.to_csv(fullname_path)
        print(f"Register have been created with: {frame_structure.shape[0]} rows")
        return frame_structure
    else:
        loadata_cvs = pd.read_csv(fullname_path).drop(["Unnamed: 0"], axis=1)
        filt = ~(frame_structure["uuid"].isin(loadata_cvs["uuid"]))
        dynamic_frame = frame_structure[filt]

        if not dynamic_frame.empty:
            dynamic_frame.to_csv(fullname_path, header=False, mode="a")
            print(f"{dynamic_frame.shape[0]} Rows have been added to the Register")
            return dynamic_frame
        else:
            print("No differences since last execution.")
            return


def storeid_table():
    db_path = (
        f'{os.environ["URL_DATABASE_DRIVER"]}://{os.environ["URL_DATABASE_USERNAME"]}:'
        f'{os.environ["URL_DATABASE_PASSW"]}@{os.environ["URL_DATABASE_HOST"]}:'
        f'{os.environ["URL_DATABASE_PORT"]}/{os.environ["URL_DATABASE_NAME"]}'
    )
    engine = create_engine(db_path)
    conn = engine.connect()
    stmt = "SELECT store_id, name FROM store"
    dataframe_storeid = pd.read_sql(stmt, conn)
    return dataframe_storeid


def store_idx(frame):
    store_idx = []
    df_storeid = storeid_table()

    for f in frame["storename"]:
        for idx, s in enumerate(df_storeid["name"]):
            if f == s:
                store_id = df_storeid["store_id"].iloc[idx]
                store_idx.append(store_id)
    return store_idx


def transform_structure(frame):
    datafr = frame.copy()
    datafr["store_id"] = store_idx(datafr)
    datafr["created"] = pd.to_datetime(datafr["date"], format="%d/%m/%Y")
    datafr.set_index(["created", "store_id"], inplace=True)

    dataframe = datafr.iloc[:, [3, 4]].stack()
    dataframe = dataframe.reset_index()

    dataframe["reseller_id"] = 1
    dataframe["customer_id"] = 18
    dataframe["update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    new_cols = list(dataframe.columns.values)
    data_set = dataframe[
        [
            new_cols[2],
            new_cols[4],
            new_cols[5],
            new_cols[1],
            new_cols[3],
            new_cols[0],
            new_cols[6],
        ]
    ]
    data_set.rename(columns={"level_2": "ticket_category", 0: "value"}, inplace=True)
    data_set.index.name = "ticket_id"

    return data_set


def modify_reg(file_reg_path):
    import datetime

    reg_path = f".temp_reg/{file_reg_path}.csv"

    file_csv = pd.read_csv(file_reg_path, sep=",")
    file_csv.drop("Unnamed: 0", axis=1, inplace=True)
    file_csv["reg_date"] = pd.to_datetime(file_csv["reg_date"])

    today = datetime.date.today()
    filt_day = file_csv["reg_date"].dt.date == today
    new_reg = file_csv.loc[~filt_day]
    new_reg.to_csv(file_reg_path)


def store_to_database(frame, str_table_name, file_reg_path):
    start_time = time.perf_counter()
    db_path = (
        f'{os.environ["URL_DATABASE_DRIVER"]}://{os.environ["URL_DATABASE_USERNAME"]}:'
        f'{os.environ["URL_DATABASE_PASSW"]}@{os.environ["URL_DATABASE_HOST"]}:'
        f'{os.environ["URL_DATABASE_PORT"]}/{os.environ["URL_DATABASE_NAME"]}'
    )
    engine = create_engine(db_path, use_batch_mode=True)
    try:
        if database_exists(db_path):
            conn = engine.connect()
            table_name = str_table_name

            frame.to_sql(
                table_name,
                conn,
                if_exists="append",
                index=True,
                chunksize=30000,
                schema="bi_syenap",
                method="multi",
            )
            conn.close()
            end_time = time.perf_counter()
            run_time = end_time - start_time

            print(
                f"DataFrame: {table_name}....Insert rows: {frame.shape[0]} in time {run_time:.4f} secs "
                f"Successfully Transferred"
            )
    except Exception as e:
        modify_reg(file_reg_path)
        print("Database not Available at this moment.")


def cross_sale_transform(ventas_fr):
    ventas_data = ventas_fr.copy()
    cross_sale_cat_dict = {
        "Equipos Libres": 1,
        "Altas Pospago": 2,
        "Altas Prepago": 3,
        "Reco": 4,
        "Migra": 5,
    }

    ventas_data["reseller_id"] = 1
    ventas_data["customer_id"] = 18
    ventas_data["cross_sale_category_id"] = ventas_data["groupname"].map(
        cross_sale_cat_dict
    )
    ventas_data["store_id"] = store_idx(ventas_data)
    ventas_data["created"] = pd.to_datetime(ventas_data["date"], format="%d/%m/%Y")
    ventas_data["update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ventas_data.drop(
        ["storename", "storeid", "date", "group", "groupname", "uuid", "reg_date"],
        axis=1,
        inplace=True,
    )
    columns_header = list(ventas_data.columns.values)

    cross_sales_fr = ventas_data[
        [
            columns_header[3],
            columns_header[1],
            columns_header[2],
            columns_header[4],
            columns_header[0],
            columns_header[5],
            columns_header[6],
        ]
    ]
    cross_sales_fr.rename(columns={"count": "value"})
    cross_sales_fr.index.name = "cross_sale_id"

    return cross_sales_fr
