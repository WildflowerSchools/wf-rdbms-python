import wf_rdbms.utils
import pandas as pd
import logging

logger = logging.getLogger(__name__)

TYPES = {
    'integer': {
        'pandas_dtype': 'Int64',
        'to_pandas_series': lambda x: pd.Series(x, dtype='Int64'),
        'to_python_list': lambda x: wf_rdbms.utils.series_to_list(pd.Series(x, dtype='Int64'))
    },
    'float': {
        'pandas_dtype': 'float',
        'to_pandas_series': lambda x: pd.Series(x, dtype='float'),
        'to_python_list': lambda x: wf_rdbms.utils.series_to_list(pd.Series(x, dtype='float'))
    },
    'string': {
        'pandas_dtype': 'string',
        'to_pandas_series': lambda x: pd.Series(x, dtype='string'),
        'to_python_list': lambda x: wf_rdbms.utils.series_to_list(pd.Series(x, dtype='string'))
    },
    'boolean': {
        'pandas_dtype': 'boolean',
        'to_pandas_series': lambda x: pd.Series(x, dtype='boolean'),
        'to_python_list': lambda x: wf_rdbms.utils.series_to_list(pd.Series(x, dtype='boolean'))
    },
    'datetime': {
        'pandas_dtype': 'datetime64[ns]',
        'to_pandas_series': lambda x: pd.to_datetime(x),
        'to_python_list': lambda x: wf_rdbms.utils.series_to_list(pd.to_datetime(x).to_pydatetime())
    },
    'date': {
        'pandas_dtype': 'object',
        'to_pandas_series': lambda x: pd.Series(x).apply(wf_rdbms.utils.to_date),
        'to_python_list': lambda x: wf_rdbms.utils.series_to_list(pd.Series(x).apply(wf_rdbms.utils.to_date))
    },
    'list': {
        'pandas_dtype': 'object',
        'to_pandas_series': lambda x: pd.Series(x).where(pd.notnull(pd.Series(x)), None),
        'to_python_list': lambda x: wf_rdbms.utils.series_to_list(d.Series(x).apply(lambda y: list(y) if pd.notnull(y) else None))
    }
}

class Database:
    """
    Class to define a generic database object
    """
    def __init__(
        self,
        name,
        tables
    ):
        """
        Contructor for Database

        The tables input should be a list of Table objects. See documentation
        for Table

        Parameters:
            name (str) The name of this database
            tables (list of Table): Tables that make up the database
        """
        self.name=name
        self.tables = {table.name: table for table in tables}

    def check_integrity(self):
        for table in self.tables.values():
            logger.info('Checking integrity of table: {}'.format(table.name))
            table.check_integrity()

class Table:
    """
    Class to define a generic table object
    """
    def __init__(
        self,
        name,
        fields,
        primary_key,
        foreign_keys=None
    ):
        """
        Contructor for Table

        A table consists of a set of fields (table columns) along with some
        table-level constraints. Currently the class supports two kinds of
        table-level constraints: a required primary key and zero or more
        optional foreign keys.

        The primary key is simply a list of the field names from this Table that
        comprise the primary key. Each foreign key is a tuple consisting of the
        name of the target table and the list of the field names from this Table
        that correspond to the primary key of the target table.

        Parameters:
            name (str): The name of this table
            fields (list of Field): Fields that make up the Table
            primary_key (list of str): Fields that comprise the primary key of this table
            foreign_keys (list of tuple): Tuples consisting of target table name and fields from this table
        """
        self.name = name
        self.fields = fields
        self.primary_key = primary_key
        self.foreign_keys = foreign_keys
        self.field_names = [field.name for field in self.fields]
        self.key_field_names = primary_key
        self.value_field_names = [field_name for field_name in self.field_names if field_name not in self.key_field_names]

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
        raise NotImplementedError('Method must be implemented by child class')

    def dataframe(self):
        """
        Returns a Pandas dataframe containing the data in the data table.

        Returns:
            (DataFrame): Pandas dataframe containing the data in the data table
        """
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
        for field in self.fields:
            dataframe[field.name] = TYPES[field.type]['to_pandas_series'](dataframe[field.name])
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
        for field in self.fields:
            if field.name in self.key_field_names:
                logger.info('Checking dtype of key field: {}'.format(field.name))
                field_dtype = df.index.get_level_values(field.name).dtype
                schema_dtype = TYPES[field.type]['pandas_dtype']
                if field_dtype != schema_dtype:
                    raise ValueError('Key field {} has dtype {} but schema specifies dtype {}'.format(
                        field.name,
                        field_dtype,
                        schema_dtype
                    ))
            else:
                logger.info('Checking dtype of value field: {}'.format(field.name))
                field_dtype = df[field.name].dtype
                schema_dtype = TYPES[field.type]['pandas_dtype']
                if field_dtype != schema_dtype:
                    raise ValueError('Value field {} has dtype {} but schema specifies dtype {}'.format(
                        field.name,
                        field_dtype,
                        schema_dtype
                    ))

class Field:
    """
    Class to define a generic field (table column) object
    """
    def __init__(
        self,
        name,
        type,
        max_len=None,
        unique=False,
        not_null=False
    ):
        """
        Contructor for Field

        A field consists of a type, an optional max length, and a set of
        optional field-level constraints.

        Types that are currently supported are 'integer', 'float', 'string',
        'boolean', 'datetime', 'date' and 'list'.

        The `max_len` option is currently only supported for the 'string' type.

        The only field-level constraints that are currently supported are
        `unique` and `not_null`. Primary and foreign key constraints are
        specified at the table level instead.

        Parameters:
            name (str): The name of this field
            type (str): The type of this field
            max_len (int): The maximum length of the data in the field [Default: None]
            unique (bool): Boolean indicating whether the values in this field must be unique [Default: False]
            not_null (bool): Boolean indicating whether null values are disallowed in this field [Defaut: False]
        """
        self.name = name
        self.type = type
        self.max_len = max_len
        self.unique = unique
        self.not_null = not_null
