from .parent import _LearnerParams, _PredictorParams
import knime.extension as knext
from util import utils as kutil


##################################################################
# """
# SARIMAX paramter settings with validations
# """
##################################################################


@knext.parameter_group(label="Sarimax Model Parameters")
class SXLearnerParams(_LearnerParams):

    """

    SARIMAX Learner Parameter class inheriting the private class _LearnerParams above.

    The learner attributes are put together in the parameter group called "Sarimax Model Parameters".

    An additional column parameter is initialized in this child class by the name, "exog_column".

    This attribute is the column selection of the exogenous variable.

    """

    exog_column = knext.ColumnParameter(
        label="Exogenous Column",
        description="Table containing exogenous column for the SARIMAX model, must contain a numeric column with no missing values.",
        port_index=0,
        column_filter=kutil.is_numeric,
    )


@knext.parameter_group(label="Sarimax Predictor Parameters")
class SXPredictorParams(_PredictorParams):

    """

    SARIMAX Predictor Parameter class inheriting the private class _PredictorParams above.

    The predictor attributes are put together in the parameter group called "Sarimax Predictor Parameters".

    An additional column parameter is initialized in this child class by the name, "exog_column_forecasts".

    This attribute is the column selection of the exogenous variable for the forecasts. Note that number of forecasts should be equal
    to the number of rows in this new exogenous column.

    """

    exog_column_forecasts = knext.ColumnParameter(
        label="Exogenous Column for Forecasting",
        description="Table containing exogenous column for making forecasts on the SARIMAX model, must contain a numeric column with no missing values and of length equal to the number of forecasts to be made.",
        port_index=1,
        column_filter=kutil.is_numeric,
    )


@knext.parameter_group(label="Sarimax Forecaster Settings")
class SarimaxForecasterParms:

    """

    SARIMAX settings to configure the parameters for the model.


    """

    # target column for modelling
    input_column = knext.ColumnParameter(
        label="Target Column",
        description="Table containing training data for fitting the SARIMAX model, must contain a numeric target column with no missing values to be used for forecasting.",
        port_index=0,
        column_filter=kutil.is_numeric,
    )

    learner_params = SXLearnerParams()
    predictor_params = SXPredictorParams()
