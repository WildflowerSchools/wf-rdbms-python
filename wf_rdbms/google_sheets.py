from .utils import to_date
from gspread_pandas import Spread, Client
import pandas as pd
import json
import datetime
import logging

logger = logging.getLogger(__name__)

def ingest_student_data_google_sheets(sheet_metadata):
    df_list = list()
    logger.info('Ingesting data from each sheet')
    for sheet_metadatum in sheet_metadata:
        pull_date = sheet_metadatum['pull_date']
        sheet_id = sheet_metadatum['sheet_id']
        df_sheet = ingest_student_data_google_sheet(sheet_id, pull_date)
        df_list.append(df_sheet)
    logger.info('Ingested data from {} sheets. Concatenating.'.format(len(df_list)))
    df = pd.concat(df_list, ignore_index=True)
    df.sort_values(['school_id_tc', 'student_id_tc', 'pull_datetime'], inplace=True, ignore_index=True)
    return df

def ingest_student_data_google_sheet(sheet_id, pull_date):
    logger.info('Ingesting data from sheet with pull date {} and ID {}'.format(pull_date, sheet_id))
    spread = Spread(sheet_id)
    df = spread.sheet_to_df(index=None)
    df['school_id_tc'] = pd.to_numeric(df['school_id']).astype('Int64')
    df['child_raw_dict'] = df['child_raw'].apply(lambda x: json.loads(x))
    df['student_id_tc'] = pd.to_numeric(df['child_raw_dict'].apply(lambda x: int(x.get('id')))).astype('Int64')
    df['pull_datetime'] = pd.to_datetime(pull_date)
    df['student_first_name'] = df['child_raw_dict'].apply(lambda x: x.get('first_name')).astype('string')
    df['student_middle_name'] = df['child_raw_dict'].apply(lambda x: x.get('middle_name')).astype('string')
    df['student_last_name'] = df['child_raw_dict'].apply(lambda x: x.get('last_name')).astype('string')
    df['student_birth_date'] = df['child_raw_dict'].apply(lambda x: to_date(x.get('birth_date')))
    df['student_gender'] = df['child_raw_dict'].apply(lambda x: x.get('gender')).astype('string')
    df['student_ethnicity'] = df['child_raw_dict'].apply(lambda x: x.get('ethnicity'))
    df['student_dominant_language'] = df['child_raw_dict'].apply(lambda x: x.get('dominant_language')).astype('string')
    df['student_household_income'] = df['child_raw_dict'].apply(lambda x: x.get('household_income')).astype('string')
    df['student_grade'] = df['child_raw_dict'].apply(lambda x: x.get('grade')).astype('string')
    df['student_classroom_ids'] = df['child_raw_dict'].apply(lambda x: x.get('classroom_ids'))
    df['student_program'] = df['child_raw_dict'].apply(lambda x: x.get('program')).astype('string')
    df['student_hours_string'] = df['child_raw_dict'].apply(lambda x: x.get('hours_string')).astype('string')
    df['student_id_tc_alt'] = df['child_raw_dict'].apply(lambda x: x.get('student_id')).astype('string')
    df['student_allergies'] = df['child_raw_dict'].apply(lambda x: x.get('allergies')).astype('string')
    df['student_parent_ids'] = df['child_raw_dict'].apply(lambda x: x.get('parent_ids'))
    df['student_approved_adults_string'] = df['child_raw_dict'].apply(lambda x: x.get('approved_adults_string')).astype('string')
    df['student_emergency_contacts_string'] = df['child_raw_dict'].apply(lambda x: x.get('emergency_contacts_string')).astype('string')
    df['student_notes'] = df['child_raw_dict'].apply(lambda x: x.get('notes')).astype('string')
    df['student_last_day'] = df['child_raw_dict'].apply(lambda x: to_date(x.get('last_day')))
    df['student_exit_reason'] = df['child_raw_dict'].apply(lambda x: x.get('exit_reason')).astype('string')
    df['student_exit_survey_id'] = pd.to_numeric(df['child_raw_dict'].apply(lambda x: x.get('exit_survey_id'))).astype('Int64')
    df['student_exit_notes'] = df['child_raw_dict'].apply(lambda x: x.get('exit_notes')).astype('string')
    df = df.reindex(columns=[
        'school_id_tc',
        'student_id_tc',
        'pull_datetime',
        'student_first_name',
        'student_middle_name',
        'student_last_name',
        'student_birth_date',
        'student_gender',
        'student_ethnicity',
        'student_dominant_language',
        'student_household_income',
        'student_grade',
        'student_classroom_ids',
        'student_program',
        'student_hours_string',
        'student_id_tc_alt',
        'student_allergies',
        'student_parent_ids',
        'student_approved_adults_string',
        'student_emergency_contacts_string',
        'student_notes',
        'student_last_day',
        'student_exit_reason',
        'student_exit_survey_id',
        'student_exit_notes'
    ])
    if df.duplicated(['school_id_tc', 'student_id_tc']).any():
        raise ValueError('Ingested data contains duplicate Transparent Classroom school ID/student id combinations')
    return df
