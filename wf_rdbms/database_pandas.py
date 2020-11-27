from wf_rdbms.database import Database, DataTable, TYPES
import pandas as pd
from collections import OrderedDict
import logging

logger = logging.getLogger(__name__)

class DatabasePandas(Database):
    """
    Class to define a Pandas database implementation
    """
    def _init(self):
        self.data_tables = OrderedDict()
        for data_table_name, data_table_schema in self.database_schema.items():
            self.data_tables[data_table_name] = DataTablePandas(data_table_schema)

class DataTablePandas(DataTable):
    """
    Class to define a Pandas data table implementation
    """
    def _init(self):
        self._df = pd.DataFrame({field_name: pd.Series([], dtype=TYPES[field_schema['type']]['pandas_dtype']) for field_name, field_schema in self.data_table_schema.items()})
        # self._df = super().type_convert_columns(self._df)
        self._df.set_index(self.key_field_names, inplace=True)

    def _create_records(self, records):
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

    def _update_records(self, records):
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

    def _delete_records(self, records):
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

    def _dataframe(self):
        return self._df

    def _index(self):
        return self._df.index
