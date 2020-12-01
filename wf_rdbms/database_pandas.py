from wf_rdbms.database import Database,Table, Field, TYPES
import pandas as pd
from collections import OrderedDict
import logging

logger = logging.getLogger(__name__)

class DatabasePandas(Database):
    """
    Class to define a Pandas database implementation
    """
    def init(self, name, tables):
        super().__init__(name, tables)

class TablePandas(Table):
    """
    Class to define a Pandas table implementation
    """
    def __init__(
        self,
        name,
        fields,
        primary_key,
        foreign_keys=None
    ):
        super().__init__(
            name,
            fields,
            primary_key,
            foreign_keys
        )
        self._df = pd.DataFrame({field.name: pd.Series([], dtype=TYPES[field.type]['pandas_dtype']) for field in self.fields})
        self._df.set_index(self.key_field_names, inplace=True)

    def create_records(self, records):
        records = super().normalize_records(records)
        already_existing_records = self._df.index.intersection(records.index)
        if len(already_existing_records) > 0:
            logger.info('Of {} specified records, {} have key values that are already in the data table. Ignoring these.'.format(
                len(records),
                len(already_existing_records)
            ))
            records.drop(already_existing_records, inplace=True)
        self._df = pd.concat((self._df, records))
        self._df.sort_index(inplace=True)
        return_key_values = list(records.index)
        logger.info('Created {} records'.format(len(return_key_values)))
        return return_key_values

    def update_records(self, records):
        records = super().normalize_records(records)
        non_existing_records = records.index.difference(self._df.index)
        if len(non_existing_records) > 0:
            logger.info('Of {} specified records, {} have key values that are not in data table. Ignoring these.'.format(
                len(records),
                len(non_existing_records)
            ))
            records.drop(non_existing_records, inplace=True)
        self._df.update(records)
        self._df.sort_index(inplace=True)
        return_key_values = list(records.index)
        logger.info('Updated {} records'.format(len(return_key_values)))
        return return_key_values

    def delete_records(self, records):
        records = super().normalize_records(records, normalize_value_columns=False)
        non_existing_records = records.index.difference(self._df.index)
        if len(non_existing_records) > 0:
            logger.info('Of {} specified records, {} have key values that are not in data table. Ignoring these.'.format(
                len(records),
                len(non_existing_records)
            ))
            records.drop(non_existing_records, inplace=True)
        self._df.drop(records.index, inplace=True)
        self._df.sort_index(inplace=True)
        return_key_values = list(records.index)
        logger.info('Deleted {} records'.format(len(return_key_values)))
        return return_key_values

    def dataframe(self):
        return self._df

    def index(self):
        return self._df.index

class FieldPandas(Field):
    """
    Class to define a Pandas field (column) object
    """
    def __init__(
        self,
        name,
        type,
        max_len=None,
        unique=False,
        not_null=False
    ):
        super().__init__(
            name,
            type,
            max_len,
            unique,
            not_null
        )
