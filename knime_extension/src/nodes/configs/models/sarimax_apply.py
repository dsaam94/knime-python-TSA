from .parent import _PredictorParams
import knime.extension as knext
from util import utils as kutil


@knext.parameter_group(label="Sarimax Predictor Parameters")
class SXPredictorApplyParams(_PredictorParams):

    """

    SARIMAX Predictor Parameter class inheriting the private class _PredictorParams above.

    The predictor attributes are put together in the parameter group called "Sarimax Predictor Parameters".

    An additional column parameter is initialized in this child class by the name, "exog_column_forecasts".

    This attribute is the column selection of the exogenous variable for the forecasts. Note that number of forecasts should be equal
    to the number of rows in this new exogenous column.

    """

    exog_column_forecasts = knext.ColumnParameter(
        label="Exogenous Column for Forecasting",
        description="The exogenous column for forecasting ",
        port_index=1,
        column_filter=kutil.is_numeric,
    )

    natural_log = knext.BoolParameter(
        label="Reverse Log",
        description="Check this option only if the target column was log transformed during training the model.",
        default_value=False,
    )


@knext.parameter_group(label="Sarimax Forecaster Parameters")
class SarimaxForecasterParms:

    """

    SARIMAX Forecaster nodes parameters put together in one complete group called "Sarimax Forecaster Parameters"

    """

    predictor_params = SXPredictorApplyParams()
