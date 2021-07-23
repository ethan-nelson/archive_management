from datetime import datetime
from geoalchemy2 import (
    Geography,
)
import hashlib
import os
from PIL import Image
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Text,
    text,
    create_engine,
    select,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import declarative_base, mapper, sessionmaker, scoped_session

from utils import calc_hash, check_extract_exif

Base = declarative_base()


class LocalFile(Base):
    __tablename__ = "local_files"
    filepath = Column(Text, primary_key=True)
    file_hash_md5 = Column(Text)
    file_hash_sha1 = Column(Text)
    added = Column(DateTime, default=text("now()"))
    last_check = Column(DateTime, default=text("now()"))
    stillexists = Column(Boolean)
    exif_coords = Column(Geography("POINTZ", 4326))
    exif_elevation = Column(Float)
    exif_heading = Column(Float)
    exif_datetime = Column(DateTime)


class RemoteFile(Base):
    __tablename__ = "remote_files"
    id = Column(BigInteger, primary_key=True)
    fileid = Column(Text)
    filename = Column(Text)
    sha1 = Column(Text)
    last_remote_check = Column(DateTime)


engine = create_engine("postgresql://postgres@localhost/files")
Base.metadata.create_all(engine)


def update(directory="/data6/"):
    engine = create_engine("postgresql://postgres@localhost/files")
    session = scoped_session(sessionmaker(bind=engine))
    for p, d, f in os.walk(directory):
        for n in f:
            fname = os.path.join(p, n)
            fname = fname.replace("'", "\'")
            if not os.path.isfile(fname):
                continue
            file_data = {}
            file_data["file_hash_md5"] = calc_hash(fname, hashlib.md5)
            file_data["file_hash_sha1"] = calc_hash(fname, hashlib.sha1)
            file_data["last_check"] = datetime.now()
            stmt = select(LocalFile).where(LocalFile.filepath == fname)
            results = session.execute(stmt).scalars().all()
            if len(results) == 0:
                if fname[-4:] == ".JPG":
                    a, b = check_extract_exif(fname)
                    if a is not None:
                        file_data["exif_datetime"] = a
                    if b is not None:
                        file_data["exif_coords"] = b
                        file_data["exif_coords"] = "POINTZ(%s %s %s)" % (
                            file_data["exif_coords"][:]
                        )
                file_data["filepath"] = fname
                input_model = {**file_data}
            else:
                if file_data["file_hash_sha1"] != results[0].__dict__["file_hash_sha1"]:
                    raise Exception("Error matching sha1 for %s" % fname)
                file_data["filepath"] = fname
            stmt = (
                insert(LocalFile)
                .values(**file_data)
                .on_conflict_do_update(
                    constraint="local_files_pkey", set_={**file_data}
                )
            )
            results = session.execute(stmt)
            session.commit()
