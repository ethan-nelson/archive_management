from datetime import datetime
import hashlib
from PIL import Image


def calc_hash(fname, hash_impl=hashlib.sha1):
    file_hash = hash_impl()
    with open(fname, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            file_hash.update(block)
    return file_hash.hexdigest()


def convert_exif(data):
    def _dms_to_dd(dms_value) -> float:
        return float(dms_value[0] + (dms_value[1] / 60.0) + (dms_value[2] / 3600.0))

    lat = _dms_to_dd(data[2])
    if data[1] == "S": lat = -1 * lat
    lon = _dms_to_dd(data[4])
    if data[3] == "W": lon = -1 * lon
    elevation = float(data[6])
    if data[5] == b"\x01": elevation = -1 * elevation

    return (lon, lat, elevation)


def check_extract_exif(fname):
    f = Image.open(fname, "r")
    exif_data = f._getexif()
    exif_datetime = exif_data.get(36867)
    exif_coords = exif_data.get(34853)
    if exif_datetime:
        exif_datetime = datetime.strptime(exif_datetime, "%Y:%m:%d %H:%M:%S")
    if exif_coords:
        exif_coords = convert_exif(exif_coords)

    return exif_datetime, exif_coords

