import sys, os
import logging
import knime_extension as knext

#create path to refer to knutils file
sys.path.append(os.path.abspath(os.path.join(os.path.pardir, '..')))
from utils import knutils as kutil


##################################################################

##################################################################


class _LearnerParams:

    """
    A private class containing common parameters for both SARIMA and SARIMAX learner along with its validation functions. 

    """

    ar_order_param = knext.IntParameter(
        label = "AR Order (p)"
        , description = "The number of lagged observations to be used in the model. "
        , default_value = 0
        , min_value=0
        )

    i_order_param = knext.IntParameter(
        label = "I Order (d)"
        , description = "The number of times to apply differencing before training the model."
        , default_value = 0
        , min_value=0
        )

    ma_order_param = knext.IntParameter(
        label = "MA Order (q)"
        , description = "The number of lagged forecast errors to be used in the model."
        , default_value = 0
        , min_value=0
        )

    seasoanal_ar_order_param = knext.IntParameter(
        label = "Seasonal AR Order (P)"
        , description = "The number of seasonally lagged observations to be used in the model."
        , default_value = 0
        , min_value=0
        )

    seasoanal_i_order_param = knext.IntParameter(
        label = "Seasonal I Order (D)"
        , description = "The number of times to apply seasonal differencing before training the model."
        , default_value = 0
        , min_value=0
        )

    seasoanal_ma_order_param = knext.IntParameter(
        label = "Seasonal MA Order (Q)"
        , description = "The number of seasonal lagged forecast errors to be used in the model. "
        , default_value = 0
        , min_value=0)

    seasonal_period_param = knext.IntParameter(
        label = "Seasonal Period"
        , description = "Specify the Length of the Seasonal Period"
        , default_value = 2
        , min_value=2
        )
    
        #validation of parameters
    def validate(self, values):

        #seasonality provided as 1 or 0
        if (values["seasonal_period_param"] < 2):
            raise knext.InvalidParametersError("Seasonality can not be less than 2.")
        
        #handle P >0 and p >= s
        if ((values["seasoanal_ar_order_param"] > 0) and (values["ar_order_param"] >= values["seasonal_period_param"])):
            raise knext.InvalidParametersError("Invalid number of autoregressive terms (p,P) defined")
        
        # handle Q > 0 and q >= s
        if ((values["seasoanal_ma_order_param"] > 0) and (values["ma_order_param"] >= values["seasonal_period_param"])):
            raise knext.InvalidParametersError("Invalid number of moving average terms (q,Q) terms defined")  


##################################################################

##################################################################

class _PredictorParams:
    """
    A private class containing common parameters for both SARIMA and SARIMAX predictors along with its validation functions.

    """

    number_of_forecasts = knext.IntParameter(
        label = "Forecast"
        , description = "Forecasts of the given time series h period ahead of the training data"
        , default_value = 1
        , min_value = 1
        )

    dynamic_check = knext.BoolParameter(
        label = "Dynamic"
        , description = "Check this box to use in-sample prediction as lagged values. Otherwise use true values "
        , default_value = True
        )

    def validate(self, values):
        if (values["number_of_forecasts"] < 1):
            raise ValueError("At least one forecast should be made by the model.")




@knext.parameter_group(label="Sarima Model Parameters")
class SLearnerParams(_LearnerParams):
    """

    Sarima Learner Parameter class inheriting the private class _LearnerParams above.

    The learner attributes are put together in the parameter group called "Sarima Model Parameters".

    """
    def __init__(self):
        super().__init__()
    

    
      

@knext.parameter_group(label="Sarima Predictor Parameters")
class SPredictorParams(_PredictorParams):
   
    """

    SARIMA Predictor Parameter class inheriting the private class _PredictorParams above.

    The predictor attributes are put together in the parameter group called "Sarima Model Parameters".

    """
    

    def __init__(self):
        super().__init__()



@knext.parameter_group(label = "Sarima Forecaster Parameters")
class SarimaForecasterParms():
    """

    SARIMA Forecaster nodes parameters put together in one complete group called "Sarima Forecaster Parameters"

    """

    learner_params = SLearnerParams()
    predictor_params = SPredictorParams()







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


    