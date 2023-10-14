import knime_extension as knext
from utils import knutils as kutil


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

