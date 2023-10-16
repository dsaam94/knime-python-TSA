import logging
import knime.extension as knext
from util import utils as kutil

LOGGER = logging.getLogger(__name__)


@knext.parameter_group(label="Visualization Parameters")
class AutocorrParams:

    target_col = knext.ColumnParameter(
        label = "Value Column"
        , description="The target value to inspect"
        , port_index = 0
        , column_filter= kutil.is_numeric
        )


    max_lag = knext.IntParameter(
        label = "Max Lag"
        , description = "Maximum lag to use when checking for (partial) autocorrelation."
        , default_value = 100
        , min_value = 1
    )