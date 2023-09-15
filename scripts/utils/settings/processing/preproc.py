import sys, os
import logging
import knime_extension as knext
import pandas as pd
#create path to refer to knutils file
sys.path.append(os.path.abspath(os.path.join(os.path.pardir, '..')))
from utils import knutils as kutil



#########################################################################
# Aggregation Granularity
#########################################################################


@knext.parameter_group(label="Aggregation Granularity Parameters")
class AggregationGranularityParams:

    # discuss timestamps with Corey
    class TimeGranularityOpts(knext.EnumParameterOptions):
        YEAR = (
            "Year",
            """Aggregate Granularity on Year""",
        )
        QUARTER = (
            "Quarter",
            "Aggregate geanularity on quarters",
        )
        MONTH = (
            "Month",
            "Aggregate granularity based on months.",
        )
        WEEK = (
            "Week",
            """Aggregate granularity based on weeks.""",
        )
        DAY = (
            "Day",
            "Aggregate granularity based on days.",
        )
        HOUR = (
            "Hour",
            """Aggregate based on hours""",
        )
        MINUTE = (
            "Minute",
            """Aggregate based on minutes, preferably from seconds""",
        )
        SECOND = (
            "Seconds",
            """Aggregate granularity based on seconds.""",
        )

        @classmethod
        def get_default(cls):
            return cls.DAY

    class AggregationMethods(knext.EnumParameterOptions):
        MODE = (
            "Mode",
            """Pick the most commonly occuring value""",
        )
        MINIMUM = (
            "Minimum",
            "Aggregate minimum value in the interval",
        )
        MAXIMUM = (
            "Maximum",
            "Aggregate maximum value in the given interval.",
        )
        SUM = (
            "Sum",
            """Sum value in the selected granularity range.""",
        )
        VARIANCE = (
            "Variance",
            "Calculate variance in the given interval",
        )
        COUNT = (
            "Count",
            """Count number of instance s in the aggregation interval""",
        )
        MEAN = (
            "Mean",
            """Calculate mean on the selected interval""",
        )

        @classmethod
        def get_default(cls):
            return cls.MEAN

    class AggregationDictionary(knext.EnumParameterOptions):
        """
        This class is the enumeration of the selected aggregation from client-side and 
        the corresponding input for the aggregate application at Pandas end.
        """

        MODE = (
            "mode",
            # return the first mode value for each group
            lambda x: pd.Series.mode(x).iat[0],
        )
        MINIMUM = (
            "minimum",
            "min",
        )
        MAXIMUM = (
            "maximum",
            "max",
        )
        SUM = (
            "sum",
            "sum",
        )
        VARIANCE = (
            "variance",
            "var",
        )
        COUNT = (
            "count",
            "count",
        )
        MEAN = (
            "mean",
            "mean",
        )

        @classmethod
        def get_default(cls):
            return cls.MEAN

    datetime_col = knext.ColumnParameter(
        label = "Date&Time_ Column"
        , description="The target date&time column"
        , port_index = 0
        , column_filter= kutil.is_type_timestamp
        )
    aggregation_column = knext.ColumnParameter(
        label = "Aggregation Column"
        , description="Column with regression column to apply aggregation on"
        , port_index=0
        , column_filter=kutil.is_numeric
    )

    time_granularity = knext.EnumParameter(
        label="Time Granularity",
        description="Select time granularity for aggregation",
        default_value=TimeGranularityOpts.get_default().name,
        enum=TimeGranularityOpts,
    )

    aggregation_methods = knext.EnumParameter(
        label = "Aggregation Method"
        , description = "Select aggregation method"
        , default_value = AggregationMethods.get_default().name
        , enum = AggregationMethods
    )

#########################################################################
# Seasonal Differencing
#########################################################################


@knext.parameter_group(label="Seasonal Differencing Parameters")
class SeasonalDifferencingParams:

    target_column = knext.ColumnParameter(
        label = "Target Column"
        , description="Target column to difference on"
        , port_index=0
        , column_filter=kutil.is_numeric
    )

    lags = knext.IntParameter(
        label = "Lag"
        , description = "Difference given column based on specified number of lags"
        , default_value = 1
        , min_value=1
        )


#########################################################################
# Timestamp Alignment
#########################################################################

@knext.parameter_group(label="Timestamp Alignment Parameters")
class TimeStampAlignmentParams:
        # discuss timestamps with Corey
    class Period(knext.EnumParameterOptions):
        YEAR = (
            "Year",
            """Impute missing years in the range""",
        )
        MONTH = (
            "Month",
            "Impute missing months in the given range",
        )
        WEEK = (
            "Week",
            """Imupte missing weeks in the given range""",
        )
        DAY = (
            "Day",
            "Impute missing dates in the given range",
        )
        HOUR = (
            "Hour",
            """Impute missing hours in the given range""",
        )
        MINUTE = (
            "Minute",
            """Impute missing minutes in the given date range""",
        )
        SECOND = (
            "Seconds",
            """Impute missing seconds in the given range""",
        )

        @classmethod
        def get_default(cls):
            return cls.HOUR

    class TimeFrequency(knext.EnumParameterOptions):
        YEAR = (
            "Year",
            "1Y",
        )
        MONTH = (
            "Month",
            "1M",
        )
        WEEK = (
            "Week",
            "1W",
        )
        DAY = (
            "Day",
            "1D",
        )
        HOUR = (
            "Hour",
            "1H",
        )
        MINUTE = (
            "Minute",
            "1Min",
        )
        SECOND = (
            "Seconds",
            "1S",
        )

        @classmethod
        def get_default(cls):
            return cls.HOUR
        
    datetime_col = knext.ColumnParameter(
        label = "Date&Time Column"
        , description="The target date&time column"
        , port_index = 0
        , column_filter= kutil.is_type_timestamp
        )

    replace_original = knext.BoolParameter(
        label = "Replace timestamp column "
        , description="A boolean check to replace the input timestamp column with processed one or not."
        , default_value= True
        )
    
    period = knext.EnumParameter(
        label="Period",
        description="Select time period for alignment",
        default_value=Period.get_default().name,
        enum=Period,
    )