import datetime
import logging
import knime_extension as knext
import pytz
from utils import knutils as kutil
import pandas as pd
import numpy as np
from utils.settings.processing.preproc import AggregationGranularityParams, TimeStampAlignmentParams, SeasonalDifferencingParams



LOGGER = logging.getLogger(__name__)

__category = knext.category(
    path="/community/ts",
    level_id="proc",
    name="Preprocessing",
    description="Nodes for pre-processing date&time.",
    # starting at the root folder of the extension_module parameter in the knime.yml file
    icon="icons/icon.png"
)



#Error updating the description, discuss with Python team
@knext.node(name="Aggregation Granularity", node_type=knext.NodeType.MANIPULATOR, icon_path="icons/icon.png", category=__category, id="aggregation_granularity")
@knext.input_table(name="Input Data", description="Table contains the date&time column and intended regression column to apply aggregation method.")
@knext.output_table(name="Aggregations", description="Aggregated output with modified date&time and selected granularity.")
class AggregationGranularity:
    """
    
    This component aggregates values in a selected numeric or string column by timestamps extracted from a column of type Date&Time. The granularity of the timestamps and the aggregation method are defined by the user. 
    
    If the selected granularity indicates time, the extracted timestamps will have the same column type as the input column. For other granularities (days, months, etc.), the timestamps will appear in a column of type Local Date. 

    For Quarter and Week granularity, the first date of the corresponding period (quarter or week) will be returned as a column of Local Date type in the output table. 

    Note: For Legacy Date&Time column please convert it first to Date&Time using the Legacy Date&Time to Date&Time node. 

    The available granularities are: 
    Years 
    Quarter
    Months 
    Week
    Days 
    Hours 
    Minutes 
    Seconds 

    The supported Date&Time types are: 
        Local Date Time 
        Zoned Date Time 
        Local Date 
        Local Time 

    The available aggregation methods are: 
        Mean 
        Mode 
        Min 
        Max 
        Sum 
        Variance 
        Count 
    """

    aggregParams = AggregationGranularityParams()


    def configure(self, configure_context:knext.ConfigurationContext, input_schema_1):

        # set date&time column by default
        self.aggregParams.datetime_col = kutil.column_exists_or_preset(
            configure_context, self.aggregParams.datetime_col, input_schema_1, kutil.is_type_timestamp
        )
        

        #set aggregation column
        self.aggregParams.aggregation_column = kutil.column_exists_or_preset(
            configure_context, self.aggregParams.aggregation_column, input_schema_1, kutil.is_numeric
        )


        return None


    def execute(self, exec_context: knext.ExecutionContext, input_1):


        
        df = input_1.to_pandas()

        
        date_time_col_orig = df[self.aggregParams.datetime_col]
        agg_col = df[self.aggregParams.aggregation_column]

        kn_date_time_format = kutil.get_type_timestamp(str(date_time_col_orig.dtype))

        # if condition to handle zoned date&time
        if(kn_date_time_format == kutil.DEF_ZONED_DATE_LABEL):

            a = kutil.cast_to_related_type(kn_date_time_format, date_time_col_orig) 

            date_time_col, kn_date_time_format, zoneOffset = a[0], a[1], a[2]
        
        else:
            #returns series of date time according to the date format and knime supported data type
            a = kutil.cast_to_related_type(kn_date_time_format, date_time_col_orig)

            #handle multiple iterable error. This is done to handle dynamic assignment of variables in case zoned date and time type is encountered  
            date_time_col, kn_date_time_format = a[0], a[1] 


        # extract date&time fields from the input timestamp column
        df_time = kutil.extract_time_fields(date_time_col, kn_date_time_format, str(date_time_col.name))
        
        # this variable is assigned the time granularity selected by the user
        selected_time_granularity =  self.aggregParams.time_granularity.lower()

        # this variable is assigned the aggregation method selected by the user
        selected_aggreg_method = self.aggregParams.aggregation_methods.lower()

        # raise exception if selected time granularity does not exists in the input timestamp column
        if selected_time_granularity not in df_time.columns:
            raise knext.InvalidParametersError(f"""Input timestamp column does not contain {selected_time_granularity} field.""")
        
        #modify the input timestamp as per the time_gran selected. This modifies the timestamp column depending on the granularity selected
        df_time_updated = self.__modify_time(selected_time_granularity, kn_date_time_format, df_time)   


        # if kn_date_time_format contains zone and if selected time granularity is less than day then append the zone back, other wise ignore
        if((kn_date_time_format == kutil.DEF_ZONED_DATE_LABEL) and 
           (selected_time_granularity in kutil.timeGranularityList())) :
 
            df_time_updated = self.__append_TimeZone(df_time_updated,zoneOffset)

        # perform final aggregation
        df_grouped = self.__aggregate(df_time_updated, agg_col, selected_time_granularity, selected_aggreg_method)
        df_grouped = df_grouped[[self.aggregParams.datetime_col, self.aggregParams.aggregation_column]]


        return knext.Table.from_pandas(df_grouped)
    
    
    def __modify_time(self,time_gran:str,kn_date_time_type:str,df_time:pd.DataFrame):
        """
        This function modifies the input timestamp column according to the type of granularity selected. So for instance if the selected time granularity is "Quarter" then 
        the next higher time value against quarter will be year. Hence only year will be returned.
        """

        df = df_time.copy()

        #check if granularity level is 
        date = df[self.aggregParams.datetime_col] 
        if (time_gran in (self.aggregParams.TimeGranularityOpts.YEAR.name.lower(),
                          self.aggregParams.TimeGranularityOpts.QUARTER.name.lower(),
                          self.aggregParams.TimeGranularityOpts.MONTH.name.lower(),
                          self.aggregParams.TimeGranularityOpts.WEEK.name.lower())
                          ):

            #return year only
            date = date.dt.year
            df[self.aggregParams.datetime_col] = date


        # rounnd input timestamp to nearest date
        elif (time_gran == self.aggregParams.TimeGranularityOpts.DAY.name.lower()):

            date = date.dt.date
            df[self.aggregParams.datetime_col] = date

        # round datetime to nearest hour
        elif (time_gran == self.aggregParams.TimeGranularityOpts.HOUR.name.lower()):

            df[self.aggregParams.datetime_col] = self.__floorTime(kn_date_time_type,"H", date)



        # round datetime to nearest minute
        elif (time_gran == self.aggregParams.TimeGranularityOpts.MINUTE.name.lower()):

            df[self.aggregParams.datetime_col] = self.__floorTime(kn_date_time_type,"min", date)



        # round datetime to nearest second. This option is feasble if timestamp contains milliseconds/microseconds/nanoseconds.
        elif (time_gran == self.aggregParams.TimeGranularityOpts.SECOND.name.lower()):
            
            df[self.aggregParams.datetime_col] = self.__floorTime(kn_date_time_type,"S", date)
 
        
        return df
        
    def __floorTime(self, kn_date_time_type:str, time_gran:str, date:pd.Series) -> pd.Series:
            
            if(kn_date_time_type == kutil.DEF_TIME_LABEL):

                date = pd.to_datetime(date, format = kutil.TIME_FORMAT)
                date = date.dt.floor(time_gran)
                date = date.dt.time

            else:
                date = pd.to_datetime(date, format = kutil.DATE_TIME_FORMAT)
                date = date.dt.floor(time_gran)
            
            
            return date




    def __aggregate(self, df_time:pd.Series, aggregation_column:pd.Series, time_gran:str, agg_type:str):
        """
        This function performs the final aggregation based on the selected level of granularity in given datetime column.
        The aggregation is done on the modified date column and the datetime field corresponding to the selected granularity. 

        """


        #pre-process
        df = pd.concat([df_time, aggregation_column], axis = 1)


        # filter out necessary columns
        df = df[[self.aggregParams.datetime_col, self.aggregParams.aggregation_column, time_gran]]

        #select granularity 
        value = self.aggregParams.AggregationDictionary[agg_type.upper()].value[1]

        # aggregate for final output
        df = df.groupby([df[self.aggregParams.datetime_col], time_gran]).agg(value).reset_index()
        return df


    def __append_TimeZone(self, date_col:pd.DataFrame, zoned:pd.Series):

        """
        This function reassignes time zones to date&time column. This function is only called if input date&time column containes time zone.
        """

        date_col_internal = date_col
        for i in range(0,len(zoned.index)):
            date_col_internal[self.aggregParams.datetime_col][i] = date_col_internal[self.aggregParams.datetime_col][i].replace(tzinfo=zoned.iloc[i])
        return date_col_internal
        


@knext.node(name="Differencing", node_type=knext.NodeType.MANIPULATOR, icon_path="icons/icon.png", category=__category, id="differencing")
@knext.input_table(name="Input Data", description="Table contains the regression column to be diffeenced")
@knext.output_table(name="Aggregations", description="Output the column having ")
class SeasonalDifferencingNode:
    """
    yet to be done
    """

    seasdiffParams = SeasonalDifferencingParams()
        
    def configure(self, configure_context:knext.ConfigurationContext, input_schema):
       
       self.seasdiffParams.target_column = kutil.column_exists_or_preset(
            configure_context, self.seasdiffParams.target_column, input_schema, kutil.is_numeric
        )
       
       return None
        
    

    def execute(self, exec_context: knext.ExecutionContext, input_table):
        df = input_table.to_pandas()

        target_col = df[self.seasdiffParams.target_column]

        target_col_new = target_col.diff(periods = self.seasdiffParams.lags)

        df[self.seasdiffParams.target_column + "(" + (str(-self.seasdiffParams.lags)) + ")"] = target_col_new

        return knext.Table.from_pandas(df)        






@knext.node(name="Timestamp Alignment", node_type=knext.NodeType.MANIPULATOR, icon_path="icons/icon.png", category=__category, id="timestamp_alignment")
@knext.input_table(name="Input Data", description="Table contains the date&time column to be diffeenced")
@knext.output_table(name="Aligned Timestamp", description="Output the column with missing timestamps in the range")
class TimestampAlignmentNode:
    """
    This component checks if the 
    """

    tsalignParams = TimeStampAlignmentParams()

    def configure(self, configure_context:knext.ConfigurationContext, input_schema):
       
        self.tsalignParams.datetime_col = kutil.column_exists_or_preset(
            configure_context, self.tsalignParams.datetime_col, input_schema, kutil.is_type_timestamp
        )
       
        return None
    
    def execute(self, exec_context: knext.ExecutionContext, input_table):

        df = input_table.to_pandas()

        date_time_col_orig = df[self.tsalignParams.datetime_col]

        kn_date_time_format = kutil.get_type_timestamp(str(date_time_col_orig.dtype))
        #LOGGER.warn(kn_date_time_format)


        # if condition to handle zoned date&time
        if(kn_date_time_format == kutil.DEF_ZONED_DATE_LABEL):
            a = kutil.cast_to_related_type(kn_date_time_format, date_time_col_orig) 

            date_time_col, kn_date_time_format, zoneOffset = a[0], a[1], a[2] 

        else:       
            #returns series of date time according to the date format and knime supported data type
            a = kutil.cast_to_related_type(kn_date_time_format, date_time_col_orig)

            #handle multiple iterable error. This is done to handle dynamic assignment of variables incase zoned date and time type is encountered  
            date_time_col, kn_date_time_format = a[0], a[1] 


        # this variable is assigned to the period selected by the user
        selected_period =  self.tsalignParams.period.lower()

        # extract date&time fields from the input timestamp column
        df_time = kutil.extract_time_fields(date_time_col, kn_date_time_format, str(date_time_col.name))

        # raise exception if selected period does not exists in the input timestamp column
        if  selected_period not in df_time.columns:
            raise knext.InvalidParametersError(f"""Input timestamp column cannot resample on {selected_period} field. Please change timestamp data type and try again.""")
        

        #check to work on timezone otherwise proceed normally
        if(kn_date_time_format == kutil.DEF_ZONED_DATE_LABEL):
            df_time_updated = self.__modify_time( kn_date_time_format, df_time, tz = zoneOffset)
        else:
            df_time_updated = self.__modify_time( kn_date_time_format, df_time)

        df = df.drop(columns = [self.tsalignParams.datetime_col])
        df = df_time_updated.merge(df, how="left", left_index=True, right_index=True)\
                            .reset_index(drop = True)\
                            .sort_values(self.tsalignParams.datetime_col + " (New)")\
                            .reset_index(drop = True)
        
        if (self.tsalignParams.replace_original):
            df = df.drop(columns = [self.tsalignParams.datetime_col])\
                   .rename(columns={self.tsalignParams.datetime_col + " (New)":self.tsalignParams.datetime_col})


        return knext.Table.from_pandas(df)
    


    def __modify_time(
            self
            ,kn_date_format:str 
            , df_time
            , tz = None
            ) -> pd.DataFrame:
        
        """
        This function is where the date column is processed to fill in for missing time stamp values
        """
        df = df_time.copy()

        start = df[self.tsalignParams.datetime_col].astype(str).min()
        end = df[self.tsalignParams.datetime_col].astype(str).max()
        frequency = self.tsalignParams.TimeFrequency[self.tsalignParams.period].value[1]
        
        timestamps = pd.date_range(start=start, end=end, freq=frequency)
        
        if (kn_date_format == kutil.DEF_TIME_LABEL):

            
            timestamps = pd.Series(timestamps.time)
            modified_dates = self.__alignTime(timestamps=timestamps, df = df)


        elif (kn_date_format == kutil.DEF_DATE_LABEL):
        
            timestamps = pd.to_datetime(pd.Series(timestamps), format=kutil.DATE_FORMAT)
            timestamps = timestamps.dt.date

            modified_dates = self.__alignTime(timestamps=timestamps, df = df)


        elif (kn_date_format == kutil.DEF_DATE_TIME_LABEL):
            timestamps = pd.to_datetime(pd.Series(timestamps), format=kutil.DATE_TIME_FORMAT)

            modified_dates = self.__alignTime(timestamps=timestamps, df = df)            
    
        elif (kn_date_format == kutil.DEF_ZONED_DATE_LABEL):

            unique_tz = pd.unique(tz.astype(str))

            LOGGER.warn("Timezones in the column:" + str(unique_tz))

            if (len(unique_tz) > 1):
                raise knext.InvalidParametersError(f"Selected date&time column contains multiple zones.")
            else:
                modified_dates = self.__alignTime(timestamps=timestamps, df = df)
                for column in modified_dates.columns:
                    modified_dates[column] = modified_dates[column].dt.tz_localize(tz[0])


            
        return modified_dates

    def __alignTime(self, timestamps:pd.Series, df:pd.DataFrame) -> pd.DataFrame:


        __duplicate = "_Dup12345"

        #find set difference from available timestamps and missing timestamps
        df2 = pd.DataFrame(set(timestamps).difference(df[self.tsalignParams.datetime_col]), columns = [self.tsalignParams.datetime_col + __duplicate])
        df2 = df2.set_index(self.tsalignParams.datetime_col + __duplicate, drop = False)

        #concatenate difference timestamps with input timestamps 
        df3 = pd.DataFrame(pd.concat([df[self.tsalignParams.datetime_col], df2[self.tsalignParams.datetime_col + __duplicate]])).rename(columns={0:self.tsalignParams.datetime_col + " (New)"})
        
        # do a left join and return only the actual time input and updated timestamp column
        new_df = df3.merge(df, how="left",left_index = True, right_index = True, sort = True)#.reset_index(drop=True)
        new_df = new_df[[self.tsalignParams.datetime_col, self.tsalignParams.datetime_col + " (New)"]]
        
        return new_df
    

