import logging
import knime_extension as knext
from utils import knutils as kutil
from utils.settings.models.sarima_x import SarimaForecasterParms
from utils.settings.models.sarima_x import SarimaxForecasterParms
import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX

LOGGER = logging.getLogger(__name__)

__category = knext.category(
    path="/community/ts",
    level_id="models",
    name="Models",
    description="Nodes for modelling Time Series",
    # starting at the root folder of the extension_module parameter in the knime.yml file
    icon="icons/icon.png",
    ##after="io",
)




@knext.node(name="SARIMA Forecaster", node_type=knext.NodeType.LEARNER, icon_path="icons/icon.png", category=__category, id="sarima")
@knext.input_table(name="Input Data", description="Table contains numeric target column to fit SARIMA")
@knext.output_table(name="Forecast", description="Forecasted values and their standard errors")
@knext.output_table(name="In-sample & Residuals", description="Residuals from the training model")

class SarimaForcaster:
    """

    This node trains a Seasonal AutoRegressive Integrated Moving Average (SARIMA) model. SARIMA models capture temporal structures in time series data in the following components: 
    - AR: Relationship between the current observation and a number (p) of lagged observations 
    - I: Degree (d) of differencing required to make the time series stationary 
    - MA: Time series mean and the relationship between the current forecast error and a number (q) of lagged forecast errors 

    *Seasonal versions of these operate similarly with lag intervals equal to the seasonal period (S). 

    Additionally, coefficent statistics and residuals are provided as table outputs. 

    Model Summary metrics: 
    RMSE (Root Mean Square Error) 
    MAE (Mean Absolute Error) 
    MAPE (Mean Absolute Percentage Error) 
    *will be missing if zeroes in target 
    R2 (Coefficient of Determination) 
    Log Likelihood 
    AIC (Akaike Information Criterion) 
    BIC (Bayesian Information Criterion) 
    """

    sarima_params = SarimaForecasterParms()


    #target column for modelling
    input_column = knext.ColumnParameter(
        label = "Target Column"
        , description="The numeric column to fit the model."
        , port_index = 0
        , column_filter=kutil.is_numeric
        )
    
    
# merge in-samples and residuals (In-Samples & Residuals)
    def configure(self, configure_context, input_schema_1):
        

        self.input_column = kutil.column_exists_or_preset(
            configure_context, self.input_column, input_schema_1, kutil.is_numeric
        )

        return  None
    

    
 
    def execute(self, exec_context: knext.ExecutionContext, input_1):

        df = input_1.to_pandas()
        regression_target= df[self.input_column]
        
        #validate if target variable has any missing values or not
        self._exec_validate(regression_target)

        #model initialization and training
        model = SARIMAX(
            regression_target
            , order=(
                self.sarima_params.learner_params.ar_order_param
                ,self.sarima_params.learner_params.i_order_param
                ,self.sarima_params.learner_params.i_order_param
                )
            , seasonal_order=(
                self.sarima_params.learner_params.seasoanal_ar_order_param
                ,self.sarima_params.learner_params.seasoanal_i_order_param
                ,self.sarima_params.learner_params.seasoanal_ma_order_param
                ,self.sarima_params.learner_params.seasonal_period_param
                )
            )
        model_fit = model.fit()


        # produce residuals
        residuals = model_fit.resid

        # in-samples
        in_samples = pd.Series(dtype = np.float64)

        in_samples = in_samples.append(model_fit.predict(start = 1, dynamic=self.sarima_params.predictor_params.dynamic_check))
        

        # combine residuals and is-samples to as part of one dataframe
        in_samps_residuals = pd.concat([residuals, in_samples], axis=1)
        in_samps_residuals.columns = ["Residuals","In-Samples"]

        # make out-of-sample forecasts
        forecasts = model_fit.forecast(steps = self.sarima_params.predictor_params.number_of_forecasts).to_frame(name="Forecasts")
        


        return knext.Table.from_pandas(forecasts), knext.Table.from_pandas(in_samps_residuals)


    #function to perform validation on dataframe within execution context
    def _exec_validate(self, column):
        
        # check for missing values first
        if(kutil.check_missing_values(column)):
            missing_count = kutil.count_missing_values(column)
            raise knext.InvalidParametersError(
                f"""There are "{missing_count}" number of missing values in the target column."""
            )
        
        # validate that enough values are being provided to train the SARIMA model
        set_val = set([
            # p
            self.sarima_params.learner_params.ar_order_param,
            # S * P 
            self.sarima_params.learner_params.seasonal_period_param * self.sarima_params.learner_params.seasoanal_ar_order_param,
            # S*Q
            self.sarima_params.learner_params.seasonal_period_param * self.sarima_params.learner_params.seasoanal_ma_order_param]
        )

        num_of_rows = kutil.number_of_rows(column)

        if(num_of_rows < max(set_val)):
            raise knext.InvalidParametersError(f"""Number of rows must be at least "{max(set_val)}" to train the model """)






####################################################################################################
# Replicating above node to incorporate Exogenous variable
####################################################################################################



@knext.node(name="SARIMAX Forecaster", node_type=knext.NodeType.LEARNER, icon_path="icons/icon.png", category=__category, id="sarimax")
@knext.input_table(name="Input Data", description="Table contains numeric target column to fit SARIMA")
@knext.input_table(name="Exogenous Input", description="Link to exogenous variable")
@knext.output_table(name="Forecast", description="Forecasted values and their standard errors")
@knext.output_table(name="In-sample & Residuals", description="Residuals from the training model")       
class SXForecaster():
    """

    This node has a descriptionTrains a Seasonal AutoRegressive Integrated Moving Average eXogenous(SARIMAX ) model. SARIMA models capture temporal structures in time series data in the following components: 
    - AR: Relationship between the current observation and a number (p) of lagged observations 
    - I: Degree (d) of differencing required to make the time series stationary 
    - MA: Time series mean and the relationship between the current forecast error and a number (q) of lagged forecast errors 

    *Seasonal versions of these operate similarly with lag intervals equal to the seasonal period (S). 

    Additionally, coefficent statistics and residuals are provided as table outputs. 

    """ 
    sarimax_params = SarimaxForecasterParms()

    
    # target column for modelling
    input_column = knext.ColumnParameter(
        label = "Target Column"
        , description="The numeric column to fit the model."
        , port_index = 0
        , column_filter=kutil.is_numeric
        ) 

    def configure(self, configure_context:knext.ConfigurationContext, input_schema_1, input_schema_2):


        # set exog column for training
        self.sarimax_params.learner_params.exog_column = kutil.column_exists_or_preset(
            configure_context, self.sarimax_params.learner_params.exog_column, input_schema_1, kutil.is_numeric
        )


        # set endogenous/target variable
        self.input_column =  kutil.column_exists_or_preset(
            configure_context, self.input_column, input_schema_1, kutil.is_numeric
        )

        
        #set exog input for forecasting
        self.sarimax_params.predictor_params.exog_column_forecasts = kutil.column_exists_or_preset(
            configure_context, self.sarimax_params.predictor_params.exog_column_forecasts, input_schema_2, kutil.is_numeric
        )

        return  None 
    
    def execute(self, exec_context: knext.ExecutionContext, input_1, input_2):
        df = input_1.to_pandas()
        exog_df = input_2.to_pandas()


        exog_var = df[self.sarimax_params.learner_params.exog_column]

        exog_var_forecasts = exog_df[self.sarimax_params.predictor_params.exog_column_forecasts]

        regression_target= df[self.input_column]

        self._exec_validate(regression_target, exog_var, exog_var_forecasts)

        #model initializytion and training
        model = SARIMAX(
            regression_target
            , order=(
                self.sarimax_params.learner_params.ar_order_param
                ,self.sarimax_params.learner_params.i_order_param
                ,self.sarimax_params.learner_params.i_order_param
                )
            , seasonal_order=(
                self.sarimax_params.learner_params.seasoanal_ar_order_param
                ,self.sarimax_params.learner_params.seasoanal_i_order_param
                ,self.sarimax_params.learner_params.seasoanal_ma_order_param
                ,self.sarimax_params.learner_params.seasonal_period_param
                ),
                exog=exog_var
            )
        # maxiters set to default
        model_fit = model.fit()
        residuals = model_fit.resid

        # in-samples
        in_samples = pd.Series(dtype = np.float64)
        in_samples = in_samples.append(model_fit.predict(start = 1, dynamic=self.sarimax_params.predictor_params.dynamic_check))
        

        # combine residuals and is-samples to as part of one dataframe
        in_samps_residuals = pd.concat([residuals, in_samples], axis=1)
        in_samps_residuals.columns = ["Residuals","In-Samples"]

        # make out-of-sample forecasts
        forecasts = model_fit.forecast(steps = self.sarimax_params.predictor_params.number_of_forecasts, exog = exog_var_forecasts).to_frame(name="Forecasts")
        


        return knext.Table.from_pandas(forecasts), knext.Table.from_pandas(in_samps_residuals)
         
     #function to perform validation on dataframe within execution context
    def _exec_validate(self, target, exog_train, exog_forecast):

       ######################################################## 
       # TARGET COLUMN CHECK 
       ########################################################
       
        # check for missing values first
        if(kutil.check_missing_values(target)):
            missing_count = kutil.count_missing_values(target)
            raise knext.InvalidParametersError(
                f"""There are {missing_count}  missing values in the target column."""
            )
        
        
        
        # validate enough values are being provided to train the SARIMA model
        set_val = set([
            # p
            self.sarimax_params.learner_params.ar_order_param,
            #s *P 
            self.sarimax_params.learner_params.seasonal_period_param * self.sarimax_params.learner_params.seasoanal_ar_order_param,
            # s*Q
            self.sarimax_params.learner_params.seasonal_period_param * self.sarimax_params.learner_params.seasoanal_ma_order_param]
        )

        num_of_rows = kutil.number_of_rows(target)

        if(num_of_rows < max(set_val)):
            raise knext.InvalidParametersError(f"""Number of rows must be at least {max(set_val)} to train the model """)        
       
       
       ######################################################## 
       # EXOGENOUS COLUMN CHECK 
       ########################################################

        # check for missing values first
        if(kutil.check_missing_values(exog_train)):
            missing_count_exog = kutil.count_missing_values(exog_train)
            raise knext.InvalidParametersError(
                f"""There are {missing_count_exog} number of missing values in the exogenous (training) column."""
            )  

        # Length of target column and exogenous column must be the same
        if(kutil.number_of_rows(exog_train) != kutil.number_of_rows(target)):
            raise knext.InvalidParametersError(
                "Length of target column and Exogenous column should be the same."
            )

       ######################################################## 
       # EXOGENOUS FORECASTS COLUMN CHECK 
       ########################################################

        # check for missing values first 
        if(kutil.check_missing_values(exog_forecast)):
            missing_count_exog_fore = kutil.count_missing_values(exog_forecast)
            raise knext.InvalidParametersError(
                f"""There are "{missing_count_exog_fore}" number of missing values in the exogenous (prediction) column."""
            )

        # check that the number of rows for exogenous input relating to forecasts and number of forecasts to be made should be equal
        if(kutil.number_of_rows(exog_forecast) != self.sarimax_params.predictor_params.number_of_forecasts):
             raise knext.InvalidParametersError(
                "The number of forecasts should be equal to the length of the exogenous input for forecasts."
            )             
        
