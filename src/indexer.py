"""
Module for indexing binary files and storing metadata in a SQLite database.
"""

import json
import sqlite3
from pathlib import Path


class SQLiteBinaryIndexer:
    """
    Builds index for the binary file by scanning each length-prefixed record and 
    extracting a key (defaults to JSON field `id`)
    """
    def __init__(self, binary_path: Path, index_path: Path):
        self.binary_path = binary_path
        self.index_path = index_path

    def build_index(self):
        conn = sqlite3.connect(self.index_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS index_table (key TEXT PRIMARY KEY, offset INTEGER, length INTEGER)")

        with open(self.binary_path, 'rb') as bin_file:
            offset = 0
            while True:
                len_bytes = bin_file.read(4)
                if not len_bytes:
                    break
                length = int.from_bytes(len_bytes, byteorder='big')
                record_bytes = bin_file.read(length)
                record = json.loads(record_bytes.decode('utf-8'))

                key = record.get('id')
                if key:
                    cur.execute("INSERT OR REPLACE INTO index_table (key, offset, length) VALUES (?, ?, ?)",
                                (key, offset + 4, length))
                offset += length + 4

        conn.commit()
        conn.close()
        return self.index_path
