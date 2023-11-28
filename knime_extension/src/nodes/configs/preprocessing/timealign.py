import knime.extension as knext
from util import utils as kutil


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
        label="Date&Time Column",
        description="The target date&time column",
        port_index=0,
        column_filter=kutil.is_type_timestamp,
    )

    replace_original = knext.BoolParameter(
        label="Replace timestamp column ",
        description="A boolean check to replace the input timestamp column with processed one or not.",
        default_value=True,
    )

    period = knext.EnumParameter(
        label="Period",
        description="Select time period for alignment",
        default_value=Period.get_default().name,
        enum=Period,
    )
