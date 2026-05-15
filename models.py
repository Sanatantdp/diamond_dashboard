import csv
import os
from decimal import Decimal, InvalidOperation

from sqlalchemy import (
    Column,
    BigInteger,
    String,
    Text,
    DECIMAL,
    DateTime,
    Numeric,
    func,
    text,
    inspect as sa_inspect,
)

from db import engine, Base, SessionLocal


class BrillianceLabData(Base):
    __tablename__ = "brilliance_lab"
    __table_args__ = {
        "mysql_engine": "InnoDB",
        "mysql_charset": "utf8mb4",
        "mysql_collate": "utf8mb4_unicode_ci",
    }

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    nid = Column(String(64), unique=True, index=True)
    shape = Column(String(64))
    price = Column(DECIMAL(14, 2))
    color = Column(String(32))
    carat = Column(DECIMAL(8, 3))
    clarity = Column(String(32))
    cut = Column(String(32))
    report = Column(String(64))
    polish = Column(String(32))
    symmetry = Column(String(32))
    depth = Column(DECIMAL(8, 3))
    table = Column("table", DECIMAL(8, 3))
    fluorescence = Column(String(32))
    list_price = Column(DECIMAL(14, 2))
    girdle = Column(String(64))
    culet = Column(String(32))
    measurement = Column(String(64))
    url = Column(Text)
    diamond_image_flag = Column(String(16))
    reportNumber = Column(String(64))
    info = Column(Text)
    alias = Column(String(128))
    fast = Column(String(32))
    created_at = Column(DateTime, server_default=func.current_timestamp())
    updated_at = Column(
        DateTime,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
    )


class LooseGrownLabData(Base):
    __tablename__ = "loose_grown_lab_data"
    __table_args__ = {
        "mysql_engine": "InnoDB",
        "mysql_charset": "utf8mb4",
        "mysql_collate": "utf8mb4_unicode_ci",
    }

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    shape = Column(String(64))
    carat = Column(DECIMAL(8, 3))
    cut = Column(String(32))
    color = Column(String(32))
    clarity = Column(String(32))
    price = Column(DECIMAL(14, 2))
    data_iid = Column(String(64))
    data_id = Column(String(64))
    sku = Column(String(64), unique=True, index=True)
    created_at = Column(DateTime, server_default=func.current_timestamp())
    updated_at = Column(
        DateTime,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
    )


class PcLabData(Base):
    __tablename__ = "pc_lab_data"
    __table_args__ = {
        "mysql_engine": "InnoDB",
        "mysql_charset": "utf8mb4",
        "mysql_collate": "utf8mb4_unicode_ci",
    }

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    diamondId = Column(String(64), unique=True, index=True)
    shape = Column(String(64))
    caratWeight = Column(DECIMAL(8, 3))
    color = Column(String(32))
    clarity = Column(String(32))
    cut = Column(String(32))
    symmetry = Column(String(32))
    polish = Column(String(32))
    deptPerc = Column(DECIMAL(8, 3))
    tablePerc = Column(DECIMAL(8, 3))
    length = Column(DECIMAL(8, 3))
    width = Column(DECIMAL(8, 3))
    depth = Column(DECIMAL(8, 3))
    ratio = Column(DECIMAL(8, 3))
    girdleMin = Column(String(64))
    girdleMax = Column(String(64))
    culet = Column(String(32))
    fancyColor = Column(String(64))
    flourIntensity = Column(String(32))
    certificateFile = Column(Text)
    finalPriceUsd = Column(DECIMAL(14, 2))
    finalPriceEur = Column(DECIMAL(14, 2))
    finalPriceGbp = Column(DECIMAL(14, 2))
    certificateNumber = Column(String(64))
    stockNumber = Column(String(64))
    diamondImage = Column(Text)
    diamondVideo = Column(Text)
    diamondType = Column(String(32))
    lab = Column(String(32))
    currencyCode = Column(String(8))
    currencySymbol = Column(String(8))
    created_at = Column(DateTime, server_default=func.current_timestamp())
    updated_at = Column(
        DateTime,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
    )


def create_all_tables():
    Base.metadata.create_all(bind=engine)
    for name in Base.metadata.tables:
        print(f"Ensured table: {name}")


AUTO_COLUMNS = {"id", "created_at", "updated_at"}


def _coerce_value(col, value):
    """Convert CSV string → typed value for the given SQLAlchemy column."""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "" or value.lower() in ("null", "none", "nan"):
            return None
    if isinstance(col.type, Numeric):
        try:
            s = str(value).replace(",", "").replace("$", "").strip()
            return Decimal(s)
        except (InvalidOperation, ValueError):
            return None
    return value


def truncate_and_load_csv(model_cls, csv_path, chunk_size=1000, logger=None):
    """
    TRUNCATE the model's table, then bulk-insert every row from `csv_path`.
    CSV header names must match model attribute names. Auto columns
    (id, created_at, updated_at) and unknown columns are ignored.
    Returns the number of rows inserted.
    """
    log = logger.info if logger else print

    if not os.path.exists(csv_path):
        log(f"[DB] CSV not found, skipping load: {csv_path}")
        return 0

    cols = {c.key: c for c in sa_inspect(model_cls).columns}
    table_name = model_cls.__tablename__

    session = SessionLocal()
    inserted = 0
    try:
        session.execute(text(f"TRUNCATE TABLE `{table_name}`"))
        session.commit()
        log(f"[DB] Truncated table `{table_name}`")

        chunk = []
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cleaned = {}
                for key, raw in row.items():
                    if key not in cols or key in AUTO_COLUMNS:
                        continue
                    cleaned[key] = _coerce_value(cols[key], raw)
                if not cleaned:
                    continue
                chunk.append(cleaned)

                if len(chunk) >= chunk_size:
                    session.bulk_insert_mappings(model_cls, chunk)
                    session.commit()
                    inserted += len(chunk)
                    chunk = []

            if chunk:
                session.bulk_insert_mappings(model_cls, chunk)
                session.commit()
                inserted += len(chunk)

        log(f"[DB] Inserted {inserted:,} rows into `{table_name}` from {os.path.basename(csv_path)}")
        return inserted
    except Exception as e:
        session.rollback()
        log(f"[DB] Failed loading `{table_name}` from CSV: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    create_all_tables()
