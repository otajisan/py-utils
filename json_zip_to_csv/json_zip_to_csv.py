import os
import sys
import json
import zipfile

import pandas as pd

from datetime import datetime as dt
from pathlib import Path


def get_current_timestamp():
    return dt.now().strftime("%Y%m%d%H%M%S")


def read_file_in_zip(input_zip):
    with zipfile.ZipFile(input_zip, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            filename = file_info.filename.encode('cp437').decode('utf-8', 'ignore')

            if '.json' not in filename:
                continue

            print(f'reading {filename}')

            with zip_ref.open(file_info.filename) as f:
                yield filename, json.loads(f.read().decode('utf-8'))


def json_to_csv(input_dir_base, input_zip):
    timestamp = get_current_timestamp()
    output_dir = f'{input_dir_base}/converted-{timestamp}'

    for filename, contents in read_file_in_zip(input_zip):
        # print(contents)
        df = pd.DataFrame(contents)

        slack_ch_dir = f"{output_dir}/{filename.split('/')[0]}"
        os.makedirs(slack_ch_dir, exist_ok=True)
        out_file = f"{output_dir}/{filename.replace('.json', '.csv')}"
        print(f'writing file: {out_file}')
        df.to_csv(out_file, sep=',', index=False, encoding='utf-8')

    return output_dir


def zip_directory(folder_path):
    zip_file_name = f"{folder_path}.zip"
    folder_path = Path(folder_path)
    with zipfile.ZipFile(zip_file_name, 'w') as zipf:
        for sub_dir_path in folder_path.iterdir():
            if sub_dir_path.is_file():
                continue
            for file_path in Path(sub_dir_path).iterdir():
                if not file_path.is_file():
                    continue
                zipf.write(file_path, arcname=file_path.name, compress_type=zipfile.ZIP_DEFLATED)

    return zip_file_name


if __name__ == '__main__':
    zip_filename = sys.argv[1]
    if not zip_filename:
        print("Usage: python json_zip_to_csv.py <input_zip_file>")
        sys.exit(1)

    input_dir_base = os.getcwd()
    input_zip = f'{input_dir_base}/{zip_filename}'

    if not os.path.exists(input_zip):
        print(f"Input zip file '{input_zip}' does not exist.")
        sys.exit(1)

    output_dir = json_to_csv(input_zip)
    result_zip_file_name = zip_directory(output_dir)
    print(f'Converted files are zipped into: {result_zip_file_name}')
