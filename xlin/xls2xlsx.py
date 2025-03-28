# pip install pyexcel pyexcel-xls pyexcel-xlsx
import os

import pyexcel as p


def convert_xls_dir_to_xlsx(data_dir: str):
    filenames = os.listdir(data_dir)
    for filename in filenames:
        if filename.endswith(".xls"):
            convert_xls_to_xlsx(os.path.join(data_dir, filename))

def convert_xls_to_xlsx(file_name: str) -> str:
    converted_filename = file_name + 'x'
    if is_xslx(file_name):
        # rename to .xlsx
        with open(file_name, 'rb') as f:
            with open(converted_filename, 'wb') as f2:
                f2.write(f.read())
        return converted_filename
    sheet = p.get_sheet(file_name=file_name)
    sheet.save_as(converted_filename)
    return converted_filename


def is_xslx(filename):
    with open(filename, 'rb') as f:
        first_four_bytes = f.read()[:4]
        return first_four_bytes == b'PK\x03\x04'

if __name__ == "__main__":
    import sys
    convert_xls_to_xlsx(sys.argv[1])
