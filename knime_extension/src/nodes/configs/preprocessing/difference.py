import knime.extension as knext
from util import utils as kutil


#########################################################################
# Seasonal Differencing
#########################################################################


@knext.parameter_group(label="Differencing Parameters")
class SeasonalDifferencingParams:
    target_column = knext.ColumnParameter(
        label="Target Column",
        description="Select a numeric column to apply differencing on.",
        port_index=0,
        column_filter=kutil.is_numeric,
    )

    lags = knext.IntParameter(
        label="Lag",
        description="Periods to shift for calculating difference in the numerical column.",
        default_value=1,
        min_value=1,
    )
