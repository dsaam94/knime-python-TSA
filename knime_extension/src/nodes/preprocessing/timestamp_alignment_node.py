import logging
import knime.extension as knext
from util import utils as kutil
import pandas as pd
from ..configs.preprocessing.timealign import TimeStampAlignmentParams
import datetime

NEW_COLUMN = " (New)"

LOGGER = logging.getLogger(__name__)


@knext.node(
    name="Date&Time Alignment",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/preprocessing/Timestamp_Alignment.png",
    category=kutil.category_processsing,
    id="timestamp_alignment",
)
@knext.input_table(
    name="Input Data",
    description="Table containing the timestamp column to fill in for the missing timestamps based on the time granularity selected.",
)
@knext.output_table(
    name="Output Data",
    description="Table output with the column with missing timestamps in the given range.",
)
class TimestampAlignmentNode:
    """
    Checks a table for non-existent timestamps and generates rows with missing values for them.

    Select a timestamp column and a time granularity. The node will verify that a record exists in your table for each value at that granularity, for example if you select hours it will check for 01:00, 02:00, 03:00… if a timestamp is not found it will be inserted and missing values generated for the remaining columns.
    The final output is sorted in ascending order of newly generated timestsamp.
    Use this in combination with the missing value node to correct missing time series data.
    This node preserves duplicated values and possibly lead to cluttered output. Therefore, in case of duplicated timestamps, we encourage using *Date&Time Granularity* node before using this node.
    """

    ts_align_params = TimeStampAlignmentParams()

    def configure(
        self, configure_context: knext.ConfigurationContext, input_schema: knext.Schema
    ):
        self.ts_align_params.datetime_col = kutil.column_exists_or_preset(
            configure_context,
            self.ts_align_params.datetime_col,
            input_schema,
            kutil.is_type_timestamp,
        )

        date_ktype = (
            input_schema[[self.ts_align_params.datetime_col]].delegate._columns[0].ktype
        )

        # Need index of input col
        index = 0
        for _ in input_schema:
            if input_schema.column_names[index] != self.ts_align_params.datetime_col:
                continue
            index = index + 1
        # if option is checked, insert new column in the index next to the selected column
        if not self.ts_align_params.replace_original:
            return input_schema.insert(
                knext.Column(
                    date_ktype,
                    self.ts_align_params.datetime_col + NEW_COLUMN,
                ),
                index,
            )
        else:
            return input_schema

    def execute(self, exec_context: knext.ExecutionContext, input_table):
        df = input_table.to_pandas()

        date_time_col_orig = df[self.ts_align_params.datetime_col]

        kn_date_time_format = kutil.get_type_timestamp(str(date_time_col_orig.dtype))

        # if condition to handle zoned date&time
        if kn_date_time_format == kutil.DEF_ZONED_DATE_LABEL:
            a = kutil.cast_to_related_type(kn_date_time_format, date_time_col_orig)

            date_time_col, kn_date_time_format, zone_offset = a[0], a[1], a[2]

        else:
            # returns series of date time according to the date format and knime supported data type
            a = kutil.cast_to_related_type(kn_date_time_format, date_time_col_orig)

            # handle multiple iterable error. This is done to handle dynamic assignment of variables incase zoned date and time type is encountered
            date_time_col, kn_date_time_format = a[0], a[1]

        # this variable is assigned to the period selected by the user
        selected_period = self.ts_align_params.period.lower()

        # extract date&time fields from the input timestamp column
        df_time = kutil.extract_time_fields(
            date_time_col, kn_date_time_format, str(date_time_col.name)
        )

        # raise exception if selected period does not exists in the input timestamp column
        if selected_period not in df_time.columns:
            raise knext.InvalidParametersError(
                f"""Input timestamp column cannot resample on {selected_period} field. Please change timestamp data type and try again."""
            )

        # check to work on timezone otherwise proceed normally
        if kn_date_time_format == kutil.DEF_ZONED_DATE_LABEL:
            df_time_updated = self.__modify_time(
                kn_date_time_format, df_time, tz=zone_offset
            )
        else:
            df_time_updated = self.__modify_time(kn_date_time_format, df_time)

        df = df.drop(columns=[self.ts_align_params.datetime_col])
        df = (
            df_time_updated.merge(df, how="left", left_index=True, right_index=True)
            .reset_index(drop=True)
            .sort_values(self.ts_align_params.datetime_col + NEW_COLUMN)
            .reset_index(drop=True)
        )

        if self.ts_align_params.replace_original:
            df = df.drop(columns=[self.ts_align_params.datetime_col]).rename(
                columns={
                    self.ts_align_params.datetime_col
                    + NEW_COLUMN: self.ts_align_params.datetime_col
                }
            )

        return knext.Table.from_pandas(df)

    def __modify_time(self, kn_date_format: str, df_time, tz=None) -> pd.DataFrame:
        """
        This function is where the date column is processed to fill in for missing time stamp values
        """
        df = df_time.copy()

        start = df[self.ts_align_params.datetime_col].astype(str).min()
        end = df[self.ts_align_params.datetime_col].astype(str).max()
        frequency = self.ts_align_params.TimeFrequency[
            self.ts_align_params.period
        ].value[1]

        timestamps = pd.date_range(start=start, end=end, freq=frequency)

        if kn_date_format == kutil.DEF_TIME_LABEL:
            timestamps = pd.Series(timestamps.time)
            modified_dates = self.__align_time(timestamps=timestamps, df=df)

        elif kn_date_format == kutil.DEF_DATE_LABEL:
            timestamps = pd.to_datetime(pd.Series(timestamps), format=kutil.DATE_FORMAT)
            timestamps = timestamps.dt.date

            modified_dates = self.__align_time(timestamps=timestamps, df=df)

        elif kn_date_format == kutil.DEF_DATE_TIME_LABEL:
            timestamps = pd.to_datetime(
                pd.Series(timestamps), format=kutil.DATE_TIME_FORMAT
            )

            modified_dates = self.__align_time(timestamps=timestamps, df=df)

        elif kn_date_format == kutil.DEF_ZONED_DATE_LABEL:
            unique_tz = pd.unique(tz.astype(str))

            LOGGER.warn("Timezones in the column:" + str(unique_tz))

            if len(unique_tz) > 1:
                raise knext.InvalidParametersError(
                    "Selected date&time column contains multiple zones."
                )
            else:
                modified_dates = self.__align_time(timestamps=timestamps, df=df)
                for column in modified_dates.columns:
                    modified_dates[column] = modified_dates[column].dt.tz_localize(
                        tz[0]
                    )

        return modified_dates

    def __align_time(self, timestamps: pd.Series, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create a new column in the existing dataframe, by doing left join on the processed column with the table.
        """
        __duplicate = "_Dup12345"

        # find set difference from available timestamps and missing timestamps
        df2 = pd.DataFrame(
            set(timestamps).difference(df[self.ts_align_params.datetime_col]),
            columns=[self.ts_align_params.datetime_col + __duplicate],
        )
        df2 = df2.set_index(self.ts_align_params.datetime_col + __duplicate, drop=False)

        # concatenate difference timestamps with input timestamps
        df3 = pd.DataFrame(
            pd.concat(
                [
                    df[self.ts_align_params.datetime_col],
                    df2[self.ts_align_params.datetime_col + __duplicate],
                ]
            )
        ).rename(columns={0: self.ts_align_params.datetime_col + NEW_COLUMN})

        # do a left join and return only the actual time input and updated timestamp column
        new_df = df3.merge(
            df, how="left", left_index=True, right_index=True, sort=True
        )  # .reset_index(drop=True)
        new_df = new_df[
            [
                self.ts_align_params.datetime_col,
                self.ts_align_params.datetime_col + NEW_COLUMN,
            ]
        ]

        return new_df
