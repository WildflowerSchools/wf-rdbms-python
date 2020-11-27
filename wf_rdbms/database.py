import wf_rdbms.utils
import pandas as pd
import logging

logger = logging.getLogger(__name__)

TYPES = {
    'integer': {
        'pandas_dtype': 'Int64',
        'converter': lambda x: x.astype('Int64')
    },
    'float': {
        'pandas_dtype': 'float',
        'converter': lambda x: x.astype('float')
    },
    'string': {
        'pandas_dtype': 'string',
        'converter': lambda x: x.astype('string')
    },
    'boolean': {
        'pandas_dtype': 'boolean',
        'converter': lambda x: x.astype('boolean')
    },
    'datetime': {
        'pandas_dtype': 'datetime64[ns]',
        'converter': pd.to_datetime
    },
    'date': {
        'pandas_dtype': 'object',
        'converter': lambda x: x.apply(wf_rdbms.utils.to_date)
    },
    'list': {
        'pandas_dtype': 'object',
        # 'converter': lambda x: [list(item) for item in x]
        'converter': lambda x: x
    }
}

class Database:
    """
    Class to define a generic database object
    """
    def __init__(
        self,
        database_schema
    ):
        """
        Contructor for Database

        The database schema input should be a dict with data table names
        as keys and data table schemas as values. See DataTable object for data
        table schema format.

        Parameters:
            database_schema (dict): Database schema
        """
        self.database_schema = database_schema
        self._init()

    def _init(self):
        raise NotImplementedError('Method must be implemented by child class')

    def check_integrity(self):
        for data_table_name, data_table in self.data_tables.items():
            logger.info('Checking integrity of data table: {}'.format(data_table_name))
            data_table.check_integrity()

class DataTable:
    """
    Class to define a generic data table object
    """
    def __init__(
        self,
        data_table_schema
    ):
        """
        Contructor for DataTable

        The data table schema should be an OrderedDict with field names as keys
        and field schemas as values. Field schemas should be dicts with the
        format {'type': TYPE_NAME, 'key': [True|False]}. Possible type names are
        'integer', 'float', 'string', 'boolean', 'datetime', 'date', or 'list'.

        Parameters:
            data_table_schema (OrderedDict): Data table schema
        """
        self.data_table_schema = data_table_schema
        self.key_field_names = list()
        self.value_field_names = list()
        for field_name, field_schema in data_table_schema.items():
            if field_schema.get('key', False):
                self.key_field_names.append(field_name)
            else:
                self.value_field_names.append(field_name)
        self._init()

    def _init(self):
        raise NotImplementedError('Method must be implemented by child class')

    def create_records(self, records):
        """
        Create records in data table.

        Input should be a Pandas data frame or an object easily convertible to a
        Pandas data frame via the pandas.DataFrame() constructor (e.g., dict of
        lists, list of dicts, etc.).

        Parameters:
            records (DataFrame): Records to create

        Returns:
            (list of tuples): Key values for created records
        """
        records = self.normalize_records(records)
        logger.info('Attempting to create {} records'.format(len(records)))
        return_key_values = self._create_records(records)
        self.check_integrity()
        return return_key_values

    def _create_records(self, records):
        raise NotImplementedError('Method must be implemented by child class')

    def update_records(self, records):
        """
        Update records in data table.

        Input should be a Pandas data frame or an object easily convertible to a
        Pandas data frame via the pandas.DataFrame() constructor (e.g., dict of
        lists, list of dicts, etc.).

        Parameters:
            records (DataFrame): Records to update

        Returns:
            (list of tuples): Key values for updated records
        """
        records = self.normalize_records(records)
        logger.info('Attempting to update {} records'.format(len(records)))
        return_key_values = self._update_records(records)
        self.check_integrity()
        return return_key_values

    def _update_records(self, records):
        raise NotImplementedError('Method must be implemented by child class')

    def delete_records(self, records):
        """
        Delete records in data table.

        Input should be a Pandas data frame or an object easily convertible to a
        Pandas data frame via the pandas.DataFrame() constructor (e.g., dict of
        lists, list of dicts, etc.). All columns other than key columns are ignored

        Parameters:
            records (DataFrame): Records to delete

        Returns:
            (list of tuples): Key values for deleted records
        """
        records = self.normalize_records(records)
        logger.info('Attempting to delete {} records'.format(len(records)))
        return_key_values = self._delete_records(records)
        self.check_integrity()
        return return_key_values

    def _delete_records(self, records):
        raise NotImplementedError('Method must be implemented by child class')

    def dataframe(self):
        """
        Returns a Pandas dataframe containing the data in the data table.

        Returns:
            (DataFrame): Pandas dataframe containing the data in the data table
        """
        dataframe = self._dataframe()
        return dataframe

    def _dataframe(self):
        raise NotImplementedError('Method must be implemented by child class')

    def keys(self):
        """
        Returns a set containing all of the key values in the data table.

        Returns:
        (set): Key values in data table
        """
        return set(self.index())

    def index(self):
        """
        Returns a Pandas index containing all key values in the data table.

        Returns:
            (Index): Pandas index containing all key values in the data table
        """
        index = self._index()
        return index

    def _index(self):
        raise NotImplementedError('Method must be implemented by child class')

    def normalize_records(self, records, normalize_value_columns=True):
        """
        Normalize records to conform to structure of data table.

        Records object should be a Pandas data frame or an object easily
        convertible to a Pandas data frame via the pandas.DataFrame()
        constructor (e.g., dict of lists, list of dicts, etc.)

        Parameters:
            records(DataFrame): Records to normalize

        Returns:
        (DataFrame): Normalized records
        """
        logger.info('Normalizing input records in preparation for database operation')
        drop=False
        if records.index.names == [None]:
            drop=True
        records = pd.DataFrame(records).reset_index(drop=drop)
        records = self.type_convert_columns(records)
        if not set(self.key_field_names).issubset(set(records.columns)):
            raise ValueError('Key columns {} missing from specified records'.format(
                set(self.key_field_names) - set(records.columns)
            ))
        records.set_index(self.key_field_names, inplace=True)
        if not normalize_value_columns:
            return records
        input_value_column_names = list(records.columns)
        spurious_value_column_names = set(input_value_column_names) - set(self.value_field_names)
        if len(spurious_value_column_names) > 0:
            logger.info('Specified records contain value column names not in data table: {}. These columns will be ignored'.format(
                spurious_value_column_names
            ))
        missing_value_column_names = set(self.value_field_names) - set(input_value_column_names)
        if len(missing_value_column_names) > 0:
            logger.info('Data table contains value column names not found in specified records: {}. These values will be empty'.format(
                missing_value_column_names
            ))
        records = records.reindex(columns=self.value_field_names)
        return records

    def type_convert_columns(self, dataframe):
        logger.info('Converting data types for input records')
        for field_name, field_schema in self.data_table_schema.items():
            dataframe[field_name] = TYPES[field_schema['type']]['converter'](dataframe[field_name])
        return dataframe

    def check_integrity(self):
        self.check_for_duplicate_keys()
        self.check_field_dtypes()

    def check_for_duplicate_keys(self):
        logger.info('Checking for duplicate keys')
        if self.keys_duplicated():
            raise ValueError('Data table contains duplicate keys')

    def keys_duplicated(self):
        return self.index().duplicated().any()

    def check_field_dtypes(self):
        df = self.dataframe()
        if len(df) == 0:
            logger.info('Data table has no entries. Skipping dtype checking.')
            return
        logger.info('Checking dtype of each field')
        for key_field_name in self.key_field_names:
            logger.info('Checking dtype of key field: {}'.format(key_field_name))
            field_dtype = df.index.get_level_values(key_field_name).dtype
            schema_dtype = TYPES[self.data_table_schema[key_field_name]['type']]['pandas_dtype']
            if field_dtype != schema_dtype:
                raise ValueError('Key field {} has dtype {} but schema specifies dtype {}'.format(
                    key_field_name,
                    field_dtype,
                    schema_dtype
                ))
        for value_field_name in self.value_field_names:
            logger.info('Checking dtype of value field: {}'.format(value_field_name))
            field_dtype = df[value_field_name].dtype
            schema_dtype = TYPES[self.data_table_schema[value_field_name]['type']]['pandas_dtype']
            if field_dtype != schema_dtype:
                raise ValueError('Value field {} has dtype {} but schema specifies dtype {}'.format(
                    value_field_name,
                    field_dtype,
                    schema_dtype
                ))
