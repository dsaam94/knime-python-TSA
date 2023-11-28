import logging
import knime.extension as knext
from util import utils as kutil
import pandas as pd
import numpy as np
from ..configs.preprocessing.aggrgran import AggregationGranularityParams

LOGGER = logging.getLogger(__name__)

__category = knext.category(
    path="/community/ts",
    level_id="proc",
    name="Preprocessing",
    description="Nodes for pre-processing date&time.",
    # starting at the root folder of the extension_module parameter in the knime.yml file
    icon="icons/icon.png",
)


@knext.node(
    name="Aggregation Granularity",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/icon.png",
    category=__category,
    id="aggregation_granularity",
)
@knext.input_table(
    name="Input Data",
    description="Table contains the date&time column and intended regression column to apply aggregation method.",
)
@knext.output_table(
    name="Aggregations",
    description="Aggregated output with modified date&time and selected granularity.",
)
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

    aggreg_params = AggregationGranularityParams()

    def configure(
        self,
        configure_context: knext.ConfigurationContext,
        input_schema_1: knext.Schema,
    ):
        # TODO Specify the output schema which depends on the selected parameters

        # TODO for you Ali: delete everything else below

        # set date&time column by default
        self.aggreg_params.datetime_col = kutil.column_exists_or_preset(
            configure_context,
            self.aggreg_params.datetime_col,
            input_schema_1,
            kutil.is_type_timestamp,
        )

        # set aggregation column
        self.aggreg_params.aggregation_column = kutil.column_exists_or_preset(
            configure_context,
            self.aggreg_params.aggregation_column,
            input_schema_1,
            kutil.is_numeric,
        )

        return None

    def execute(self, exec_context: knext.ExecutionContext, input_1):
        df = input_1.to_pandas()

        date_time_col_orig = df[self.aggreg_params.datetime_col]
        agg_col = df[self.aggreg_params.aggregation_column]

        kn_date_time_format = kutil.get_type_timestamp(str(date_time_col_orig.dtype))

        # if condition to handle zoned date&time
        if kn_date_time_format == kutil.DEF_ZONED_DATE_LABEL:
            a = kutil.cast_to_related_type(kn_date_time_format, date_time_col_orig)

            date_time_col, kn_date_time_format, zone_offset = a[0], a[1], a[2]

        else:
            # returns series of date time according to the date format and knime supported data type
            a = kutil.cast_to_related_type(kn_date_time_format, date_time_col_orig)

            # handle multiple iterable error. This is done to handle dynamic assignment of variables in case zoned date and time type is encountered
            date_time_col, kn_date_time_format = a[0], a[1]

        # extract date&time fields from the input timestamp column
        df_time = kutil.extract_time_fields(
            date_time_col, kn_date_time_format, str(date_time_col.name)
        )

        # this variable is assigned the time granularity selected by the user
        selected_time_granularity = self.aggreg_params.time_granularity.lower()

        # this variable is assigned the aggregation method selected by the user
        selected_aggreg_method = self.aggreg_params.aggregation_methods.lower()

        # raise exception if selected time granularity does not exists in the input timestamp column
        if selected_time_granularity not in df_time.columns:
            raise knext.InvalidParametersError(
                f"""Input timestamp column does not contain {selected_time_granularity} field."""
            )

        # modify the input timestamp as per the time_gran selected. This modifies the timestamp column depending on the granularity selected
        df_time_updated = self.__modify_time(
            selected_time_granularity, kn_date_time_format, df_time
        )

        # if kn_date_time_format contains zone and if selected time granularity is less than day then append the zone back, other wise ignore
        if (kn_date_time_format == kutil.DEF_ZONED_DATE_LABEL) and (
            selected_time_granularity in kutil.time_granularity_list()
        ):
            df_time_updated = self.__append_time_zone(df_time_updated, zone_offset)

        # perform final aggregation
        df_grouped = self.__aggregate(
            df_time_updated, agg_col, selected_time_granularity, selected_aggreg_method
        )
        df_grouped = df_grouped[
            [self.aggreg_params.datetime_col, self.aggreg_params.aggregation_column]
        ]

        return knext.Table.from_pandas(df_grouped)

    def __modify_time(
        self, time_gran: str, kn_date_time_type: str, df_time: pd.DataFrame
    ):
        """
        This function modifies the input timestamp column according to the type of granularity selected. So for instance if the selected time granularity is "Quarter" then
        the next higher time value against quarter will be year. Hence only year will be returned.
        """

        df = df_time.copy()

        # check if granularity level is
        date = df[self.aggreg_params.datetime_col]
        if time_gran in (
            self.aggreg_params.TimeGranularityOpts.YEAR.name.lower(),
            self.aggreg_params.TimeGranularityOpts.QUARTER.name.lower(),
            self.aggreg_params.TimeGranularityOpts.MONTH.name.lower(),
            self.aggreg_params.TimeGranularityOpts.WEEK.name.lower(),
        ):
            # return year only
            date = date.dt.year
            df[self.aggreg_params.datetime_col] = date
            df[self.aggreg_params.datetime_col] = df[
                self.aggreg_params.datetime_col
            ].astype(np.int32)

        # rounnd input timestamp to nearest date
        elif time_gran == self.aggreg_params.TimeGranularityOpts.DAY.name.lower():
            date = date.dt.date
            df[self.aggreg_params.datetime_col] = date

        # round datetime to nearest hour
        elif time_gran == self.aggreg_params.TimeGranularityOpts.HOUR.name.lower():
            df[self.aggreg_params.datetime_col] = self.__floor_time(
                kn_date_time_type, "H", date
            )

        # round datetime to nearest minute
        elif time_gran == self.aggreg_params.TimeGranularityOpts.MINUTE.name.lower():
            df[self.aggreg_params.datetime_col] = self.__floor_time(
                kn_date_time_type, "min", date
            )

        # round datetime to nearest second. This option is feasble if timestamp contains milliseconds/microseconds/nanoseconds.
        elif time_gran == self.aggreg_params.TimeGranularityOpts.SECOND.name.lower():
            df[self.aggreg_params.datetime_col] = self.__floor_time(
                kn_date_time_type, "S", date
            )

        return df

    def __floor_time(
        self, kn_date_time_type: str, time_gran: str, date: pd.Series
    ) -> pd.Series:
        if kn_date_time_type == kutil.DEF_TIME_LABEL:
            date = pd.to_datetime(date, format=kutil.TIME_FORMAT)
            date = date.dt.floor(time_gran)
            date = date.dt.time

        else:
            date = pd.to_datetime(date, format=kutil.DATE_TIME_FORMAT)
            date = date.dt.floor(time_gran)

        return date

    def __aggregate(
        self,
        df_time: pd.Series,
        aggregation_column: pd.Series,
        time_gran: str,
        agg_type: str,
    ):
        """
        This function performs the final aggregation based on the selected level of granularity in given datetime column.
        The aggregation is done on the modified date column and the datetime field corresponding to the selected granularity.

        """

        # pre-process
        df = pd.concat([df_time, aggregation_column], axis=1)

        # filter out necessary columns
        df = df[
            [
                self.aggreg_params.datetime_col,
                self.aggreg_params.aggregation_column,
                time_gran,
            ]
        ]

        # select granularity
        value = self.aggreg_params.AggregationDictionary[agg_type.upper()].value[1]

        # aggregate for final output
        df = (
            df.groupby([df[self.aggreg_params.datetime_col], time_gran])
            .agg(value)
            .reset_index()
        )
        return df

    def __append_time_zone(self, date_col: pd.DataFrame, zoned: pd.Series):
        """
        This function reassignes time zones to date&time column. This function is only called if input date&time column containes time zone.
        """

        date_col_internal = date_col
        for i in range(0, len(zoned.index)):
            date_col_internal[self.aggreg_params.datetime_col][i] = date_col_internal[
                self.aggreg_params.datetime_col
            ][i].replace(tzinfo=zoned.iloc[i])
        return date_col_internal
