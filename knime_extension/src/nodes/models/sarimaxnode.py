import logging
import knime.extension as knext
from util import utils as kutil
from ..configs.models.sarimax import SarimaxForecasterParms
import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX
import pickle

LOGGER = logging.getLogger(__name__)


@knext.node(
    name="SARIMAX Forecaster",
    node_type=knext.NodeType.LEARNER,
    icon_path="icons/models/SARIMAX_Forecaster.png",
    category=kutil.category_models,
    id="sarimax",
)
@knext.input_table(
    name="Input Data",
    description="Table containing training data for fitting the SARIMAX model, must contain a numeric target column with no missing values to be used for forecasting. Additionally, this table must also contain the exogenous column to be used for training the SARIMAX model.",
)
@knext.input_table(
    name="Exogenous Input",
    description="Table containing the exogenous column for forecasting on the SARIMAX model. It must contain a numeric column with no missing values.",
)
@knext.output_table(
    name="Forecast",
    description="Table containing forecasts for the configured column, the first value will be 1 timestamp ahead of the final training value used. ",
)
@knext.output_table(
    name="In-sample & Residuals",
    description="In sample model prediction values and residuals i.e. difference between observed value and the predicted output.",
)
@knext.output_table(
    name="Model Summary",
    description="Table containing fitted model coefficients, variance of residuals (sigma2), and several model metrics along with their standard errors.",
)
@knext.output_binary(
    name="Trained model",
    description="Pickled model object that can be used by the SARIMAX (Apply) node to generate different forecast lengths without refitting the model.",
    id="sarimax.model",
)
class SXForecaster:
    """
    Trains and generates a forecast with a (S)ARIMAX Model.

    # SARIMAX Model Training

    Trains and generates a forecast using a Seasonal AutoRegressive Integrated Moving Average with eXogenous (SARIMAX) terms model. The SARIMAX models captures temporal structures in time series data in the following components:

    - **AR (AutoRegressive):** Relationship between the current observation and a number (p) of lagged observations.

    - **I (Integrated):** Degree (d) of differencing required to make the time series stationary.

    - **MA (Moving Average):** Time series mean and the relationship between the current forecast error and a number (q) of lagged forecast errors.

    *Seasonal versions of these components operate similarly, with lag intervals equal to the seasonal period (S).*

    Additionally, this node requires an *exogenous* column that externally influences the model. This column must be provided both for model training and forecasting. However, ensure that the number of rows in the exogenous variable to be used for forecasts must be equal to the number of forecasts to be made.
    Ensure that neither of the selected columns in the node configuration dialogue must contain a missing value.


    """

    sarimax_params = SarimaxForecasterParms()

    def configure(
        self,
        configure_context: knext.ConfigurationContext,
        input_schema_1,
        input_schema_2,
    ):
        # set exog column for training
        self.sarimax_params.learner_params.exog_column = kutil.column_exists_or_preset(
            configure_context,
            self.sarimax_params.learner_params.exog_column,
            input_schema_1,
            kutil.is_numeric,
        )

        # set endogenous/target variable
        self.sarimax_params.input_column = kutil.column_exists_or_preset(
            configure_context,
            self.sarimax_params.input_column,
            input_schema_1,
            kutil.is_numeric,
        )

        # set exog input for forecasting
        self.sarimax_params.predictor_params.exog_column_forecasts = (
            kutil.column_exists_or_preset(
                configure_context,
                self.sarimax_params.predictor_params.exog_column_forecasts,
                input_schema_2,
                kutil.is_numeric,
            )
        )

        forecast_schema = knext.Column(knext.double(), "Forecasts")
        insamp_res_schema = knext.Schema(
            [knext.double(), knext.double()], ["Residuals", "In-Samples"]
        )
        model_summary_schema = knext.Column(knext.double(), "value")
        binary_model_schema = knext.BinaryPortObjectSpec("sarimax.model")

        return (
            forecast_schema,
            insamp_res_schema,
            model_summary_schema,
            binary_model_schema,
        )

    def execute(self, exec_context: knext.ExecutionContext, input_1, input_2):
        df = input_1.to_pandas()
        exog_df = input_2.to_pandas()

        exog_var = df[self.sarimax_params.learner_params.exog_column]

        exog_var_forecasts = exog_df[
            self.sarimax_params.predictor_params.exog_column_forecasts
        ]

        regression_target = df[self.sarimax_params.input_column]

        self._exec_validate(regression_target, exog_var, exog_var_forecasts)

        # model initialization and training
        model = SARIMAX(
            regression_target,
            order=(
                self.sarimax_params.learner_params.ar_order_param,
                self.sarimax_params.learner_params.i_order_param,
                self.sarimax_params.learner_params.i_order_param,
            ),
            seasonal_order=(
                self.sarimax_params.learner_params.seasoanal_ar_order_param,
                self.sarimax_params.learner_params.seasoanal_i_order_param,
                self.sarimax_params.learner_params.seasoanal_ma_order_param,
                self.sarimax_params.learner_params.seasonal_period_param,
            ),
            exog=exog_var,
        )
        # maxiters set to default
        model_fit = model.fit()
        residuals = model_fit.resid

        # in-samples
        in_samples = pd.Series(dtype=np.float64)
        in_samples = in_samples.append(
            model_fit.predict(
                start=1, dynamic=self.sarimax_params.predictor_params.dynamic_check
            )
        )

        # check if log transformation is enabled
        if self.sarimax_params.learner_params.natural_log:
            val = kutil.check_negative_values(regression_target)

            # raise error if target column contains negative values
            if val > 0:
                raise knext.InvalidParametersError(
                    f" There are '{val}' non-positive values in the target column."
                )

            regression_target = np.log(regression_target)

        # combine residuals and is-samples to as part of one dataframe
        in_samps_residuals = pd.concat([residuals, in_samples], axis=1)
        in_samps_residuals.columns = ["Residuals", "In-Samples"]

        # reverse log transformation for in-sample values
        if self.sarimax_params.learner_params.natural_log:
            in_samples = np.exp(in_samples)

        # make out-of-sample forecasts
        forecasts = model_fit.forecast(
            steps=self.sarimax_params.predictor_params.number_of_forecasts,
            exog=exog_var_forecasts,
        ).to_frame(name="Forecasts")

        # reverse log transformation for forecasts
        if self.sarimax_params.learner_params.natural_log:
            forecasts = np.exp(forecasts)

        # populate model coefficients
        model_summary = self.model_summary(model_fit)

        model_binary = pickle.dumps(model_fit)

        return (
            knext.Table.from_pandas(forecasts),
            knext.Table.from_pandas(in_samps_residuals),
            knext.Table.from_pandas(model_summary),
            bytes(model_binary),
        )

    # function to perform validation on dataframe within execution context
    def _exec_validate(self, target, exog_train, exog_forecast):
        ########################################################
        # TARGET COLUMN CHECK
        ########################################################

        # check for missing values first
        if kutil.check_missing_values(target):
            missing_count = kutil.count_missing_values(target)
            raise knext.InvalidParametersError(
                f"""There are {missing_count}  missing values in the target column."""
            )

        # validate enough values are being provided to train the SARIMA model
        set_val = set(
            [
                # p
                self.sarimax_params.learner_params.ar_order_param,
                # s *P
                self.sarimax_params.learner_params.seasonal_period_param
                * self.sarimax_params.learner_params.seasoanal_ar_order_param,
                # s*Q
                self.sarimax_params.learner_params.seasonal_period_param
                * self.sarimax_params.learner_params.seasoanal_ma_order_param,
            ]
        )

        num_of_rows = kutil.number_of_rows(target)

        if num_of_rows < max(set_val):
            raise knext.InvalidParametersError(
                f"""Number of rows must be greater than maximum lag: "{max(set_val)}" to train the model. The maximum lag is the max of p, q, s*P, and s*Q."""
            )

        ########################################################
        # EXOGENOUS COLUMN CHECK
        ########################################################

        # check for missing values first
        if kutil.check_missing_values(exog_train):
            missing_count_exog = kutil.count_missing_values(exog_train)
            raise knext.InvalidParametersError(
                f"""There are {missing_count_exog} missing values in the exogenous column selected for training."""
            )

        # Length of target column and exogenous column must be the same
        if kutil.number_of_rows(exog_train) != kutil.number_of_rows(target):
            raise knext.InvalidParametersError(
                "Length of target column and exogenous column should be the same."
            )

        ########################################################
        # EXOGENOUS FORECASTS COLUMN CHECK
        ########################################################

        # check for missing values first
        if kutil.check_missing_values(exog_forecast):
            missing_count_exog_fore = kutil.count_missing_values(exog_forecast)
            raise knext.InvalidParametersError(
                f"""There are {missing_count_exog_fore} missing values in the exogenous column selected for forecasting."""
            )

        # check that the number of rows for exogenous input relating to forecasts and number of forecasts to be made should be equal
        if (
            kutil.number_of_rows(exog_forecast)
            != self.sarimax_params.predictor_params.number_of_forecasts
        ):
            raise knext.InvalidParametersError(
                "The number of forecasts should be equal to the number of rows in the exogenous input for forecasts."
            )

    def model_summary(self, model):
        # estimates of the parameter coefficients
        coeff = model.params.to_frame()

        # calculate standard deviation of the parameters in the coefficients
        coeff_errors = model.bse.to_frame().reset_index()
        coeff_errors["index"] = coeff_errors["index"].apply(lambda x: x + " Std. Err")
        coeff_errors = coeff_errors.set_index("index")

        # extract log likelihood of the trained model
        log_likelihood = pd.DataFrame(
            data=model.llf, index=["Log Likelihood"], columns=[0]
        )

        # extract AIC (Akaike Information Criterion)
        aic = pd.DataFrame(data=model.aic, index=["AIC"], columns=[0])

        # extract BIC (Bayesian Information Criterion)
        bic = pd.DataFrame(data=model.bic, index=["BIC"], columns=[0])

        # extract Mean Squared Error
        mse = pd.DataFrame(data=model.mse, index=["MSE"], columns=[0])

        # extract Mean Absolute error
        mae = pd.DataFrame(data=model.mae, index=["MAE"], columns=[0])

        summary = pd.concat(
            [coeff, coeff_errors, log_likelihood, aic, bic, mse, mae]
        ).rename(columns={0: "value"})

        return summary
