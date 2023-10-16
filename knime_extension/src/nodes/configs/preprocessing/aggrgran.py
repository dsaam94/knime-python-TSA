import knime.extension as knext
from util import utils as kutil



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