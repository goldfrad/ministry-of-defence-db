from abc import ABC

from src import db_api
import shelve
from pathlib import Path
from typing import Any, Dict, List
import os

DB_ROOT = Path('db_files')
tables = dict()


class DBTable(db_api.DBTable):

    def count(self) -> int:
        db = shelve.open(os.path.join(DB_ROOT, self.name), writeback=True)
        len_ = len(db) - 1
        db.close()
        return len_

    def insert_record(self, values: Dict[str, Any]) -> None:
        db = shelve.open(os.path.join(DB_ROOT, self.name), writeback=True)
        for value in values.keys():
            if value == self.key_field_name:
                if db.get(str(values[value])):
                    raise ValueError
                db[str(values[value])] = values
                break
        else:
            raise ValueError
        db.close()
        return None

    def delete_record(self, key: Any) -> None:
        db = shelve.open(os.path.join(DB_ROOT, self.name), writeback=True)
        try:
            if not db.get(str(key)):
                db.close()
                raise ValueError
            db.pop(str(key))
        finally:
            db.close()

    def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:
        db = shelve.open(os.path.join(DB_ROOT, self.name), writeback=True)
        for key, value in list(db.items())[1:]:
            flag = True
            for criterion in criteria:
                if criterion.field_name not in value.keys():
                    db[key][criterion.field_name] = None
                if criterion.operator == "=":
                    criterion.operator = "=="
                if not eval(str(value[criterion.field_name]) + criterion.operator + str(criterion.value)):
                    flag = False
                    break
            if flag:
                self.delete_record(key)
        db.close()

    def get_record(self, key: Any) -> Dict[str, Any]:
        db = shelve.open(os.path.join(DB_ROOT, self.name), writeback=True)
        record = db.get(str(key))
        db.close()
        return record

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        db = shelve.open(os.path.join(DB_ROOT, self.name), writeback=True)
        db[str(key)] = values
        db.close()
        return None

    def query_table(self, criteria: List[db_api.SelectionCriteria]):
        # -> List[Dict[str, Any]]:
        db = shelve.open(os.path.join(DB_ROOT, self.name), writeback=True)
        for row in db:
            flag = True
            for criterion in criteria:
                if criterion.field_name not in db[row].keys():
                    db[row][criterion.field_name] = None
                if criterion.operator == "=":
                    criterion.operator = "=="

                if not eval(db[row][criterion.field_name] + criterion.operator + criterion.value):
                    flag = False
                    break
            if flag:
                self.delete_record(row.key)
        db.close()

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


class DataBase(db_api.DataBase, ABC):
    def create_table(self,
                     table_name: str,
                     fields: List[db_api.DBField],
                     key_field_name: str) -> DBTable:

        if tables.get(table_name):
            raise ValueError

        for i in fields:
            if i.name == key_field_name:
                break
        else:
            raise ValueError

        db = shelve.open(os.path.join(DB_ROOT, table_name), writeback=True)
        db['fields'] = fields

        db_table = DBTable(table_name, fields, key_field_name)
        tables[db_table.name] = db_table
        db.close()
        return db_table

    def num_tables(self) -> int:
        return len(tables)

    def get_table(self, table_name: str) -> DBTable:
        if tables.get(table_name):
            return tables[table_name]
        else:
            raise ValueError

    def delete_table(self, table_name: str) -> None:
        if tables.get(table_name):
            os.remove(os.path.join(DB_ROOT, table_name + '.bak'))
            os.remove(os.path.join(DB_ROOT, table_name + '.dat'))
            os.remove(os.path.join(DB_ROOT, table_name + '.dir'))
            tables.pop(table_name)
        else:
            raise ValueError

    def get_tables_names(self) -> List[Any]:
        return list(tables.keys())
