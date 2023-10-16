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
        label = "Exogenous Column"
        , description="The exogenous column for training "
        , port_index = 0
        , column_filter=kutil.is_numeric
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
        label = "Exogenous Column for Forecasting"
        , description="The exogenous column for forecasting "
        , port_index = 1
        , column_filter=kutil.is_numeric
        )

              


@knext.parameter_group(label = "Sarimax Forecaster Parameters")
class SarimaxForecasterParms():

    """

    SARIMAX Forecaster nodes parameters put together in one complete group called "Sarimax Forecaster Parameters"

    """    
    learner_params = SXLearnerParams()
    predictor_params = SXPredictorParams()


    