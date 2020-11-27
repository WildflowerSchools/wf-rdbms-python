from .utils import to_date
import requests
import pandas as pd
import json
import datetime
import logging
import os

logger = logging.getLogger(__name__)

class TransparentClassroomClient:
    def __init__(
        self,
        username=None,
        password=None,
        api_token=None,
        url_base='https://www.transparentclassroom.com/api/v1/'
    ):
        self.username = username
        self.password = password
        self.api_token = api_token
        self.url_base = url_base
        if self.api_token is None:
            self.api_token = os.getenv('TRANSPARENT_CLASSROOM_API_TOKEN')
        if self.api_token is None:
            logger.info('Transparent Classroom API token not specified. Attempting to generate token.')
            if self.username is None:
                self.username = os.getenv('TRANSPARENT_CLASSROOM_USERNAME')
            if self.username is None:
                raise ValueError('Transparent Classroom username not specified')
            if self.password is None:
                self.password = os.getenv('TRANSPARENT_CLASSROOM_PASSWORD')
            if self.password is None:
                raise ValueError('Transparent Classroom password not specified')
            output_json = self.transparent_classroom_request(
                'authenticate.json',
                auth=(self.username, self.password)
            )
            self.api_token = output_json['api_token']

    def fetch_student_data(self):
        logger.info('Fetching student data from Transparent Classroom for all schools')
        school_ids = self.fetch_school_ids()
        logger.info('Fetched {} school IDs'.format(len(school_ids)))
        student_data_school_dfs = list()
        for school_id in school_ids:
            student_data_school_df = self.fetch_student_data_school(school_id)
            student_data_school_dfs.append(student_data_school_df)
        student_data_df = pd.concat(student_data_school_dfs, ignore_index=True)
        return student_data_df

    def fetch_student_data_school(self, school_id):
        logger.info('Fetching student data from Transparent Classroom for school ID {}'.format(school_id))
        output_json = self.transparent_classroom_request('children.json', school_id=school_id)
        student_data_school_df = pd.DataFrame(output_json, dtype='object')
        student_data_school_df['school_id_tc'] = int(school_id)
        student_data_school_df = student_data_school_df.reindex(
            columns= [
                'school_id_tc',
                'id',
                'first_name',
                'middle_name',
                'last_name',
                'birth_date',
                'gender',
                'ethnicity',
                'dominant_language',
                'household_income',
                'grade',
                'classroom_ids',
                'program',
                'hours_string',
                'student_id',
                'allergies',
                'parent_ids',
                'approved_adults_string',
                'emergency_contacts_string',
                'notes',
                'last_day',
                'exit_reason',
                'exit_survey_id',
                'exit_notes'
            ],
            fill_value=None
        )
        student_data_school_df.rename(
            columns= {
                'id': 'student_id_tc',
                'first_name': 'student_first_name',
                'last_name': 'student_last_name',
                'birth_date': 'student_birth_date',
                'gender': 'student_gender',
                'hours_string': 'student_hours_string',
                'dominant_language': 'student_dominant_language',
                'allergies': 'student_allergies',
                'ethnicity': 'student_ethnicity',
                'household_income': 'student_household_income',
                'approved_adults_string': 'student_approved_adults_string',
                'emergency_contacts_string': 'student_emergency_contacts_string',
                'classroom_ids': 'student_classroom_ids',
                'parent_ids': 'student_parent_ids',
                'program': 'student_program',
                'middle_name': 'student_middle_name',
                'grade': 'student_grade',
                'last_day': 'student_last_day',
                'exit_reason': 'student_exit_reason',
                'student_id': 'student_id_tc_alt',
                'notes': 'student_notes',
                'exit_survey_id': 'student_exit_survey_id',
                'exit_notes': 'student_exit_notes'
            },
            inplace=True
        )
        student_data_school_df['student_birth_date'] = student_data_school_df['student_birth_date'].apply(to_date)
        student_data_school_df['student_last_day'] = student_data_school_df['student_last_day'].apply(to_date)
        student_data_school_df = student_data_school_df.astype({
            'school_id_tc': 'Int64',
            'student_id_tc': 'Int64',
            'student_first_name': 'string',
            'student_middle_name': 'string',
            'student_last_name': 'string',
            'student_birth_date': 'object',
            'student_gender': 'string',
            'student_ethnicity': 'object',
            'student_dominant_language': 'string',
            'student_household_income': 'string',
            'student_grade': 'string',
            'student_classroom_ids': 'object',
            'student_program': 'string',
            'student_hours_string': 'string',
            'student_id_tc_alt': 'string',
            'student_allergies': 'string',
            'student_parent_ids': 'object',
            'student_approved_adults_string': 'string',
            'student_emergency_contacts_string': 'string',
            'student_notes': 'string',
            'student_last_day': 'object',
            'student_exit_reason': 'string',
            'student_exit_survey_id': 'Int64',
            'student_exit_notes': 'string'
        })
        return student_data_school_df

    def fetch_school_ids(self):
        output_json = self.transparent_classroom_request('schools.json')
        school_ids = list()
        for school in output_json:
            if school.get('type') == 'School':
                school_ids.append(int(school.get('id')))
        return school_ids

    def fetch_school_data(self):
        output_json = self.transparent_classroom_request('schools.json')
        school_data_df = pd.DataFrame(output_json)
        school_data_df = school_data_df.loc[school_data_df['type'] == "School"].copy()
        school_data_df.rename(
            columns= {
                'id': 'tc_school_id',
                'name': 'tc_school_name',
                'address': 'tc_school_address',
                'phone': 'tc_school_phone',
                'time_zone': 'tc_school_time_zone'
            },
            inplace=True
        )
        school_data_df['tc_school_id'] = school_data_df['tc_school_id'].astype('Int64')
        school_data_df['tc_school_name'] = school_data_df['tc_school_name'].astype('string')
        school_data_df['tc_school_address'] = school_data_df['tc_school_address'].astype('string')
        school_data_df['tc_school_phone'] = school_data_df['tc_school_phone'].astype('string')
        school_data_df['tc_school_time_zone'] = school_data_df['tc_school_time_zone'].astype('string')
        school_data_df = school_data_df.reindex(columns=[
                'tc_school_id',
                'tc_school_name',
                'tc_school_address',
                'tc_school_phone',
                'tc_school_time_zone'
        ])
        return school_data_df

    def transparent_classroom_request(
        self,
        endpoint,
        params=None,
        school_id=None,
        masquerade_id=None,
        auth=None
    ):
        headers = dict()
        if self.api_token is not None:
            headers['X-TransparentClassroomToken'] = self.api_token
        if school_id is not None:
            headers['X-TransparentClassroomSchoolId'] = str(school_id)
        if masquerade_id is not None:
            headers['X-TransparentClassroomMasqueradeId'] = str(masquerade_id)
        r = requests.get(
            '{}{}'.format(self.url_base, endpoint),
            params=params,
            headers=headers,
            auth=auth
        )
        if r.status_code != 200:
            error_message = 'Transparent Classroom request returned error'
            if r.json().get('errors') is not None:
                error_message += '\n{}'.format(json.dumps(r.json().get('errors'), indent=2))
            raise Exception(error_message)
        return r.json()
