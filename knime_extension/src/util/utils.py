"""
Several utility functions are reused from Harvard's spatial data lab repository for Geospatial Analytics Extension.
https://github.com/spatial-data-lab/knime-geospatial-extension/blob/main/knime_extension/src/util/knime_utils.py
"""
import knime.extension as knext
import pandas as pd
from datetime import datetime, date, time, timedelta
from typing import Callable, Union, Tuple
import logging


LOGGER = logging.getLogger(__name__)

############################################
# Date &/or time value types
############################################

TYPE_DATE_TIME = knext.logical(datetime)
TYPE_DATE = knext.logical(date)
TYPE_TIME = knext.logical(time)
TYPE_TIME_DELTA = knext.logical(timedelta)


############################################
# Column selection helper
############################################


# pre-define values factory types
ZONED_DATE_TIME_ZONE_VALUE = "org.knime.core.data.v2.time.ZonedDateTimeValueFactory2"
LOCAL_TIME_VALUE = "org.knime.core.data.v2.time.LocalTimeValueFactory"
LOCAL_DATE_VALUE = "org.knime.core.data.v2.time.LocalDateValueFactory"
LOCAL_DATE_TIME_VALUE = "org.knime.core.data.v2.time.LocalDateTimeValueFactory"


DEF_ZONED_DATE_LABEL = "ZonedDateTimeValueFactory2"
DEF_DATE_LABEL = "LocalDateValueFactory"
DEF_TIME_LABEL = "LocalTimeValueFactory"
DEF_DATE_TIME_LABEL = "LocalDateTimeValueFactory"

# formats corresponding to timestamp type
ZONED_DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S%z"
DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M:%S"



 # Filter columns visible in the column_param for numeric ones
def is_numeric(column: knext.Column) -> bool: 
    """
    Checks if column is numeric e.g. int, long or double.
    @return: True if Column is numeric
    """
    return (
        column.ktype == knext.double()
        or column.ktype == knext.int32()
        or column.ktype == knext.int64()
    )


def is_zoned_datetime(column: knext.Column) -> bool:
    """
    Checks if date&time column contains has the timezone or not.
    @return: True if selected date&time column has time zone
    """
    return __is_type_x(column, ZONED_DATE_TIME_ZONE_VALUE)


def is_datetime(column: knext.Column) -> bool:
    """
    Checks if a column is of type Date&Time.
    @return: True if selected column is of type date&time
    """
    return __is_type_x(column, LOCAL_DATE_TIME_VALUE)



def is_time(column: knext.Column)-> bool:
    """
    Checks if a column is of type Time only.
    @return: True if selected column has only time.
    """
    return __is_type_x(column, LOCAL_TIME_VALUE)



def is_date(column: knext.Column)-> bool:
    """
    Checks if a column is of type date only.
    @return: True if selected column has date only.
    """
    return __is_type_x(column, LOCAL_DATE_VALUE)



def boolean_or(*functions):
    """
    Return True if any of the given functions returns True
    @return: True if any of the functions returns True
    """

    def new_function(*args, **kwargs):
        return any(f(*args, **kwargs) for f in functions)

    return new_function


def is_type_timestamp(column: knext.Column):

    """
    This function checks on all the supported timestamp columns supported in KNIME.
    Note that legacy date&time types are not supported.
    @return: True if date&time column is compatible with the respective logical types supported in KNIME.
    """

    return boolean_or(is_time, is_date, is_datetime, is_zoned_datetime)(column)


def __is_type_x(column: knext.Column, type: str) -> bool:
    """
    Checks if column contains the given type whereas type can be :
    DateTime, Date, Time, ZonedDateTime
    @return: True if Column Type is of type timestamp
    """

    return (
        isinstance(column.ktype, knext.LogicalType)
        and type in column.ktype.logical_type
    )

# this method will check the input type for timestamp
def check_type(column: str, schema: knext.Schema):
    
    """
    This function returns the name of the factory data type for relevant timestamp column.
    @return: Name of factory data type for timestamp
    """

    selected_col = schema[column]
    type_detected =  selected_col.ktype.logical_type.split(".")
    return type_detected[len(type_detected) - 1].replace('"}', "")

############################################
# Date&Time helper methods
############################################



def convert_timestamp(value):
    """
    This function converts knime compatible datetime values
    into pandas Timestamp.
    @return: Panda's Timestamp converted date&time value 
    """
    return pd.Timestamp(value)


def extract_zone(value):

    """
    This function extracts the time zone from each timestamp value in the pandas timmestamp column.
    @return: timezone of Timestamp 
    """    
    return value.tz


def localize_timezone(value, zone):

    """
    This function updates the Pandas Timestamp value with the time zone. If "None" is passed timezone will be removed from
    timestamp returning a timezone naive value.
    @return: assigns timezone to timestamp.

    """   
    return value.tz_localize(zone)


def time_granularity_list() -> list:

    """
    This function returns list of possible time fields relevant to only Time type values. 
    @return: list of item fields in Time
    """     
    return [
        "hour",
        "minute",
        "second",
        #not supported yet
        "millisecond",
        "microsecond"
    ]



def cast_to_related_type(value_type:str, column:pd.Series):
    """
    This function converts the KNIME's date&time column to Pandas native date&time column. The format for the Pandas datetime
    is selected based on the respective KNIME's date&time logical type.
    @return: Pandas datetime Series and corresponding name of the Knime's date&time factory datatype
    """

    # parse date time with zones
    if(DEF_ZONED_DATE_LABEL == value_type):

 
        column = column.apply(convert_timestamp)

        zone_offset = column.apply(extract_zone)

        s_dateimezone = column.apply(localize_timezone, zone=None)

        s_dateimezone = pd.to_datetime(s_dateimezone, format=ZONED_DATE_TIME_FORMAT)

        return s_dateimezone, DEF_ZONED_DATE_LABEL, zone_offset

    # parse dates only
    elif(DEF_DATE_LABEL == value_type):

        s_date = pd.to_datetime(column, format = DATE_FORMAT)

        return s_date, DEF_DATE_LABEL
    
    # cast only time objects
    elif(DEF_TIME_LABEL == value_type):
        s_time = pd.to_datetime(column, format = TIME_FORMAT)

        return s_time.dt.time, DEF_TIME_LABEL


    #parse date & time
    elif( DEF_DATE_TIME_LABEL == value_type):
        s_datetime = pd.to_datetime(column, format = DATE_TIME_FORMAT)

        return s_datetime, DEF_DATE_TIME_LABEL
    


def extract_time_fields(date_time_col:pd.Series, date_time_format:str, series_name:str) -> pd.DataFrame:
    """
    This function exracts the timestamp fields in seperate columns.
    @return: Pandas datafram with timestamp column and relevant date&time fields.
    """
    

    if(date_time_format == DEF_ZONED_DATE_LABEL):
        df = pd.to_datetime(date_time_col, format = ZONED_DATE_TIME_FORMAT).to_frame()

        df["zone"] = str(date_time_col.dt.tz)
        df["year"] = df[series_name].dt.year
        df["quarter"] = df[series_name].dt.quarter
        df["month"] = df[series_name].dt.month
        df["week"] = df[series_name].dt.week
        df["day"] = df[series_name].dt.day
        df["hour"] = df[series_name].dt.hour
        df["minute"] = df[series_name].dt.minute
        df["second"] = df[series_name].dt.second

        return  df

    elif(date_time_format == DEF_DATE_LABEL):

        df = pd.to_datetime(date_time_col, format = DATE_FORMAT).to_frame()

        df["year"] = df[series_name].dt.year
        df["quarter"] = df[series_name].dt.quarter
        df["month"] = df[series_name].dt.month
        df["week"] = df[series_name].dt.week
        df["day"] = df[series_name].dt.day

        df[series_name] = df[series_name].dt.date

        return df

    elif(date_time_format == DEF_TIME_LABEL):

        df = pd.to_datetime(date_time_col, format = TIME_FORMAT).to_frame()

        df["hour"] = df[series_name].dt.hour
        df["minute"] = df[series_name].dt.minute
        df["second"] = df[series_name].dt.second

        #ensure to do this in the end
        df[series_name] = df[series_name].dt.time

        return df
    
    elif(date_time_format == DEF_DATE_TIME_LABEL):

        df = pd.to_datetime(date_time_col, format = DATE_TIME_FORMAT).to_frame()

        df["year"] = df[series_name].dt.year
        df["quarter"] = df[series_name].dt.quarter
        df["month"] = df[series_name].dt.month
        df["week"] = df[series_name].dt.week
        df["day"] = df[series_name].dt.day
        df["hour"] = df[series_name].dt.hour
        df["minute"] = df[series_name].dt.minute
        df["second"] = df[series_name].dt.second

        return df






def get_type_timestamp(value_type):
    """
    This function parses the complete name of KNIME's date&time factory method and returns the actual name of the factory data type.
    """
    typs = [ZONED_DATE_TIME_ZONE_VALUE
           , LOCAL_TIME_VALUE
           , LOCAL_DATE_VALUE
           ,LOCAL_DATE_TIME_VALUE
           ]

    for typ in typs:
        if str(value_type).__contains__(typ):
            type_detected =  typ.split(".")
            return type_detected[len(type_detected) - 1]





############################################
# General Helper Class
############################################


def column_exists_or_preset(
    context: knext.ConfigurationContext,
    column: str,
    schema: knext.Schema,
    func: Callable[[knext.Column], bool] = None,
    none_msg: str = "No compatible column found in input table",
) -> str:
    """
    Checks that the given column is not None and exists in the given schema. If none is selected it returns the
    first column that is compatible with the provided function. If none is compatible it throws an exception.
    """
    if column is None:
        for c in schema:
            if func(c):
                context.set_warning(f"Preset column to: {c.name}")
                return c.name
        raise knext.InvalidParametersError(none_msg)
    __check_col_and_type(column, schema, func)
    return column


def __check_col_and_type(
    column: str,
    schema: knext.Schema,
    check_type: Callable[[knext.Column], bool] = None,
) -> None:
    """
    Checks that the given column exists in the given schema and that it matches the given type_check function.
    """
    # Check that the column exists in the schema and that it has a compatible type
    try:
        existing_column = schema[column]
        if check_type is not None and not check_type(existing_column):
            raise knext.InvalidParametersError(
                f"Column '{str(column)}' has incompatible data type"
            )
    except IndexError:
        raise knext.InvalidParametersError(
            f"Column '{str(column)}' not available in input table"
        )


############################################
# Generic pandas dataframe/series helper function
############################################

def check_missing_values(column:pd.Series) -> bool :
    """
    This function checks for missing values in the Pandas Series.
    @return: True is missing values exist in column
    """
    return column.hasnans

def count_missing_values(column:pd.Series) -> int:
    """
    This function counts the number of missing values in the Pandas Series.
    @return: sum of boolean 1s if missing value exists.
    """
    return column.isnull().sum()

def number_of_rows(df:pd.Series) -> int:

    """
    This function returns the number of rows in the dataframe.
    @return: numerical value, denoting length of Pandas Series.
    """    
    return len(df.index)

def check_negative_values(column: pd.Series) -> int:

    total_neg = (column <= 0).sum()

    return total_neg

