"""
Binary Parquet Converter Module
"""

import json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq


class BinaryConverter:
    """
    Streams NDJSON -> custom binary file using length-prefixed records.
    """
    def __init__(self, output_path: Path):
        self.output_path = output_path

    def convert(self, input_json_path: str):
        with open(input_json_path, 'r') as json_file, open(self.output_path, 'wb') as bin_file:
            for line in json_file:
                record = line.strip().encode('utf-8')
                length = len(record)
                bin_file.write(length.to_bytes(4, byteorder='big'))
                bin_file.write(record)
        return self.output_path


class ParquetConverter:
    """
    Streams NDJSON -> Parquet file using pyarrow.
    """
    def __init__(self, output_path: Path):
        self.output_path = output_path

    def convert(self, input_json_path: str):
        records = []
        with open(input_json_path, 'r') as json_file:
            for line in json_file:
                record = json.loads(line.strip())
                records.append(record)

        table = pa.Table.from_pylist(records)
        pq.write_table(table, self.output_path)
        return self.output_path
