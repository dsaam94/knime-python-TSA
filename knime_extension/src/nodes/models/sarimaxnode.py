import logging
import knime_extension as knext
from utils import knutils as kutil
from ..configs.models.sarimax import SarimaxForecasterParms
from ..configs.models.sarimax import SarimaxForecasterParms
import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX
import pickle

LOGGER = logging.getLogger(__name__)

__category = knext.category(
    path="/community/ts",
    level_id="models",
    name="Models",
    description="Nodes for modelling Time Series",
    # starting at the root folder of the extension_module parameter in the knime.yml file
    icon="icons/icon.png"
)
        



####################################################################################################
# Replicating above node to incorporate Exogenous variable
####################################################################################################



@knext.node(name="SARIMAX Forecaster", node_type=knext.NodeType.LEARNER, icon_path="icons/icon.png", category=__category, id="sarimax")
@knext.input_table(name="Input Data", description="Table contains numeric target column to fit SARIMA")
@knext.input_table(name="Exogenous Input", description="Link to exogenous variable")
@knext.output_table(name="Forecast", description="Forecasted values and their standard errors")
@knext.output_table(name="In-sample & Residuals", description="Residuals from the training model")
@knext.output_table(name="Model Summary", description="Table containing coefficient statistics and other criterion.")
@knext.output_binary(name="Trained model",description="Model for SARIMAX", id="sarimax.model") 
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
        
        #check if log transformation is enabled
        if (self.sarimax_params.learner_params.natural_log):

            val = kutil.check_negative_values(regression_target)

            #raise error if target column contains negative values
            if (val > 0):
                raise knext.InvalidParametersError(f" There are '{val}' non-positive values in the target column.")  

            regression_target = np.log(regression_target)      

        # combine residuals and is-samples to as part of one dataframe
        in_samps_residuals = pd.concat([residuals, in_samples], axis=1)
        in_samps_residuals.columns = ["Residuals","In-Samples"]

        # reverse log transformation for in-sample values 
        if (self.sarimax_params.learner_params.natural_log):
            in_samples = np.exp(in_samples)

        # make out-of-sample forecasts
        forecasts = model_fit.forecast(steps = self.sarimax_params.predictor_params.number_of_forecasts, exog = exog_var_forecasts).to_frame(name="Forecasts")

        # reverse log transformation for forecasts
        if (self.sarimax_params.learner_params.natural_log):
            forecasts = np.exp(forecasts)



        # populate model coefficients
        model_summary = self.model_summary(model_fit)

        model_binary = pickle.dumps(model_fit)

        return knext.Table.from_pandas(forecasts), knext.Table.from_pandas(in_samps_residuals), knext.Table.from_pandas(model_summary), bytes(model_binary)
         
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
        
    def model_summary(self, model):

        # estimates of the parameter coefficients
        coeff = model.params.to_frame()

        # calculate standard deviation of the parameters in the coefficients
        coeff_errors = model.bse.to_frame().reset_index()
        coeff_errors["index"] = coeff_errors["index"].apply(lambda x : x + " Std. Err")
        coeff_errors = coeff_errors.set_index("index")

        # extract log likelihood of the trained model
        log_likelihood = pd.DataFrame(data = model.llf, index =["Log Likelihood"], columns = [0])

        # extract AIC (Akaike Information Criterion)
        aic = pd.DataFrame(data = model.aic, index =["AIC"], columns = [0])

        # extract BIC (Bayesian Information Criterion)
        bic = pd.DataFrame(data = model.bic, index =["BIC"], columns = [0])  

        # extract Mean Squared Error
        mse  = pd.DataFrame(data = model.mse, index =["MSE"], columns = [0])

        # extract Mean Absolute error
        mae  = pd.DataFrame(data = model.mae, index =["MAE"], columns = [0])

        summary = pd.concat([coeff, coeff_errors, log_likelihood, aic, bic, mse, mae]).rename(columns={0:"value"})

        return summary