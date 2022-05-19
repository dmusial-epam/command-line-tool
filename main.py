import csv
import json
import os
from collections import Counter
from datetime import datetime

import typer
from typing import Tuple, List


app = typer.Typer()

DOCKER_FOLDER = "scripts"
DB_FILE = "db.json"
FILE_EXTENSION = "log"

INVALID_FILETYPE_MSG = f"Error: Invalid file format. %s must be a .{FILE_EXTENSION} file."
INVALID_PATH_MSG = "Error: Invalid file path/name. Path %s does not exist."


def __valid_filetype(path: str):
    return path.endswith(f".{FILE_EXTENSION}")


def __valid_path(path: str):
    return os.path.exists(path)


def __make_temp_file(file_name: str, data: str):
    f = open(f"/{DOCKER_FOLDER}/{file_name}", "w+")
    f.write(data)


def __method_load():
    return json.load


def __method_save():
    return json.dumps


def __custom_enumerate(sequence, start=0):
    n = start
    for elem in sequence:
        if elem:
             yield n, elem
             n += 1


def _read_db():
    f = open(f"/{DOCKER_FOLDER}/{DB_FILE}")
    return __method_load()(f)


def _get_freq_field(column: int, reverse=False):
    db = _read_db()
    counter_ips = Counter(d[f"field_{column}"] for d in db)
    ips = counter_ips.most_common()
    if reverse:
        ips = ips[::-1]
    count = ips[0][1]
    for ip in ips:
        if ip[1] != count:
            break
        print(ip[0])
    print(f"All ips were {count} times")


def _validate_file(path: str):
    if not __valid_path(path):
        print(INVALID_PATH_MSG % path)
        return False
    elif not __valid_filetype(path):
        print(INVALID_FILETYPE_MSG % path)
        return False
    return True


def _get_files_from_dir():
    ret = []
    for file in os.listdir(f"/{DOCKER_FOLDER}/"):
        if file.endswith(f".{FILE_EXTENSION}"):
            ret.append(file)
    return ret


@app.command()
def load(
    file_names: List[str] = typer.Argument(
        None,
        help="Separate files with a space or . for all in directory"
    )
):
    ret = []
    idr = -1
    if file_names[0] == ".":
        file_names = _get_files_from_dir()
    for file_name in file_names:
        docker_path = f"/{DOCKER_FOLDER}/{file_name}"
        if not _validate_file(docker_path):
            continue
        with open(docker_path, "r", newline='') as csvfile:
            # For another serializer method this should be separate
            reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            for i, row in __custom_enumerate(reader):
                ret.append({})
                idr += 1
                for idc, column in __custom_enumerate(row, 1):
                    ret[idr][f"field_{idc}"] = column
        __make_temp_file(DB_FILE, __method_save()(ret))
    if ret:
        print("File successfully loaded")


@app.command()
def show():
    # Docker folder
    print(os.listdir(f"/{DOCKER_FOLDER}"))


@app.command()
def save(file_name: str):
    with (
        open(f"/{DOCKER_FOLDER}/{DB_FILE}", "rb") as src, 
        open(f"/{DOCKER_FOLDER}/{file_name}", "wb") as dst
    ):
        for row in src:
            dst.write(row)
    print("The file was saved successfully")


@app.command()
def most_freq_ip(column=3):
    _get_freq_field(column)


@app.command()
def least_freq_ip(column=3):
    _get_freq_field(column, True)


@app.command()
def total_bytes(columns: Tuple[int, int] = typer.Argument((2, 5))):
    db = _read_db()
    sum_bytes = sum(
        sum(
            int(row[f"field_{col}"])
            for col in columns
            if row[f"field_{col}"].isdigit()
        )
        for row in db
    )
    print(f"{sum_bytes} bytes")


@app.command()
def events_per_second(column=1):
    """
        What does this mean exactly?
    Assumptions:
        Take min, max timestamp and all records.
        Calculate X records for the difference between the times above.
    """
    db = _read_db()
    name = f"field_{column}"
    max_timestamp = max(db, key=lambda x: x[name])
    min_timestamp = min(db, key=lambda x: x[name])
    try:
        diff = (
            datetime.fromtimestamp(float(max_timestamp[name])) -
            datetime.fromtimestamp(float(min_timestamp[name]))
        ).total_seconds()
    except ValueError:
        print("Invalid timestamp")
        return
    print(len(db) / diff)


if __name__ == "__main__":
    app()
