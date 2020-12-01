import pandas as pd

def to_date(object):
    try:
        date = pd.to_datetime(object).date()
        if pd.isnull(date):
            date = None
    except:
        date = None
    return date

def series_to_list(object):
    return object.where(pd.notnull(object), None).tolist()
