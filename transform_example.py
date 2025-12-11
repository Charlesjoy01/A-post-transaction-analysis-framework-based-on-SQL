import os
import re
import warnings
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Text
import argparse
import pymysql

warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message="Workbook contains no default style.*",
)

def read_source(path, header=None):
    if path.lower().endswith(".xlsx"):
        try:
            return pd.read_excel(path)
        except ImportError:
            raise RuntimeError("读取 .xlsx 需要 openpyxl，请安装或改用 .csv")
    return pd.read_csv(path, encoding="utf-8-sig", header=header)

def sanitize_columns(df):
    cols = []
    for c in df.columns:
        s = re.sub(r"\s+", "_", str(c).strip())
        s = re.sub(r"[^0-9a-zA-Z_]", "", s)
        if not s:
            s = "col"
        cols.append(s)
    df.columns = cols
    return df

def build_engine(user, password, host, port, db=None):
    if db:
        url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
    else:
        url = f"mysql+pymysql://{user}:{password}@{host}:{port}/?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True)

def ensure_database(server_engine, db_name):
    with server_engine.begin() as conn:
        conn.execute(text("SET NAMES utf8mb4"))
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
        try:
            conn.execute(text(f"ALTER DATABASE {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
        except Exception:
            pass

def dtype_map_for(df):
    object_cols = list(df.select_dtypes(include=["object"]).columns)
    return {col: Text(collation="utf8mb4_unicode_ci") for col in object_cols}

def parse_table_columns(def_path):
    with open(def_path, "r", encoding="utf-8") as f:
        s = f.read()
    m = re.search(r"\(\s*([\s\S]*?)\)\s*;", s)
    if not m:
        raise RuntimeError("未能解析表结构列定义")
    body = m.group(1)
    lines = []
    for raw in body.splitlines():
        t = raw.strip()
        if not t:
            continue
        if t.startswith("--"):
            continue
        lines.append(t)
    cols = []
    for ln in lines:
        ln = re.sub(r",\s*$", "", ln)
        parts = re.split(r"\s+", ln)
        name = parts[0]
        if name.upper() in {"DISTKEY"}:
            continue
        if name.upper() in {"ENCODE", "BYTEDICT", "AZ64", "RAW"}:
            continue
        if name == ")":
            continue
        if name:
            cols.append(name)
    return cols

def read_csv_with_columns(csv_path, columns):
    df = read_source(csv_path, header=None)
    if df.shape[1] != len(columns):
        raise RuntimeError(f"列数不匹配: 文件 {df.shape[1]} 列, 目标 {len(columns)} 列")
    df.columns = columns
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=os.getenv("DB_NAME", "example_database"))
    parser.add_argument("--user", default=os.getenv("DB_USER", "example_user"))
    parser.add_argument("--password", default=os.getenv("DB_PASSWORD", "example_password"))
    parser.add_argument("--host", default=os.getenv("DB_HOST", "example_host"))
    parser.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", "3306")))
    parser.add_argument("--if-exists", choices=["fail", "replace", "append"], default="replace")
    parser.add_argument("--chunksize", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--pagsmile-file", default="example_pagsmile_orders.csv")
    parser.add_argument("--pagsmile-def", default="example_pagsmile_orders_table.txt")
    parser.add_argument("--transfersmile-file", default="example_transfersmile_orders.csv")
    parser.add_argument("--transfersmile-def", default="example_transfersmile_orders_table.txt")
    parser.add_argument("--table-prefix", default=os.getenv("TABLE_PREFIX", "example_"))
    args = parser.parse_args()

    server_engine = build_engine(args.user, args.password, args.host, args.port)
    ensure_database(server_engine, args.db)
    engine = build_engine(args.user, args.password, args.host, args.port, args.db)

    with engine.begin() as conn:
        conn.execute(text("SET NAMES utf8mb4"))

    tasks = []
    if os.path.exists(args.pagsmile_file) and os.path.exists(args.pagsmile_def):
        tasks.append((args.pagsmile_file, args.pagsmile_def, f"{args.table_prefix}pagsmile_orders_raw"))
    if os.path.exists(args.transfersmile_file) and os.path.exists(args.transfersmile_def):
        tasks.append((args.transfersmile_file, args.transfersmile_def, f"{args.table_prefix}transfersmile_payouts_raw"))
    if not tasks:
        raise FileNotFoundError("未发现可导入的数据源或表结构定义")

    for csv_path, def_path, table_name in tasks:
        cols = parse_table_columns(def_path)
        df = read_csv_with_columns(csv_path, cols)
        dtypes = dtype_map_for(df)
        if args.dry_run:
            print(f"✅ 干跑: {os.path.basename(csv_path)} -> {table_name} 行数 {len(df)} 列数 {len(df.columns)}")
            continue
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=args.if_exists,
            index=False,
            dtype=dtypes,
            method="multi",
            chunksize=args.chunksize,
        )
        print(f"✅ 导入完成: {os.path.basename(csv_path)} -> `{table_name}` 行数 {len(df)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ 导入失败，错误信息如下：")
        print(e)
