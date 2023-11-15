import logging
import knime.extension as knext
from util import utils as kutil
import pandas as pd
from ..configs.preprocessing.timealign import TimeStampAlignmentParams

NEW_COLUMN = " (New)"

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
    name="Timestamp Alignment",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/icon.png",
    category=__category,
    id="timestamp_alignment",
)
@knext.input_table(
    name="Input Data",
    description="Table contains the date&time column to be diffeenced",
)
@knext.output_table(
    name="Aligned Timestamp",
    description="Output the column with missing timestamps in the range",
)
class TimestampAlignmentNode:
    """
    This component aligns timestamp with the selected granularity
    """

    ts_align_params = TimeStampAlignmentParams()

    def configure(self, configure_context: knext.ConfigurationContext, input_schema):
        self.ts_align_params.datetime_col = kutil.column_exists_or_preset(
            configure_context,
            self.ts_align_params.datetime_col,
            input_schema,
            kutil.is_type_timestamp,
        )

        return None

    def execute(self, exec_context: knext.ExecutionContext, input_table):
        df = input_table.to_pandas()

        date_time_col_orig = df[self.ts_align_params.datetime_col]

        kn_date_time_format = kutil.get_type_timestamp(str(date_time_col_orig.dtype))
        # LOGGER.warn(kn_date_time_format)

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
