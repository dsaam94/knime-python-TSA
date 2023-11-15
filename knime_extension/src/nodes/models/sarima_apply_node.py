import logging
import knime.extension as knext
from util import utils as kutil
from ..configs.models.sarima_apply import SarimaForecasterParms
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
    icon="icons/icon.png",
)


@knext.node(
    name="SARIMA Forecaster (Apply)",
    node_type=knext.NodeType.LEARNER,
    icon_path="icons/icon.png",
    category=__category,
    id="sarima_apply",
)
@knext.input_binary(
    name="Model Input",
    description="Binary input of the trained SARIMA model",
    id="sarima.model",
)
@knext.output_table(
    name="Forecast", description="Forecasted values and their standard errors"
)
@knext.output_table(
    name="In-sample & Residuals", description="Residuals from the training model"
)
@knext.output_table(
    name="Model Summary",
    description="Table containing coefficient statistics and other criterion.",
)
class SarimaForcasterApply:
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

    # merge in-samples and residuals (In-Samples & Residuals)
    def configure(self, configure_context, input_schema_1):
        if (
            self.sarima_params.predictor_params.natural_log
            and self.sarima_params.predictor_params.dynamic_check
        ):
            configure_context.set_warning(
                "Enabling dynamic predictions with log transformation can cause invalid predictions."
            )

        forecast_schema = knext.Column(knext.double(), "Forecasts")
        insamp_res_schema = knext.Schema(
            [knext.double(), knext.double()], ["Residuals", "In-Samples"]
        )
        model_summary_schema = knext.Column(knext.double(), "value")

        return (forecast_schema, insamp_res_schema, model_summary_schema)

    def execute(self, exec_context: knext.ExecutionContext, model_input):
        model_fit = pickle.loads(model_input)

        # produce residuals
        residuals = model_fit.resid

        # in-samples
        in_samples = pd.Series(dtype=np.float64)

        in_samples = in_samples.append(
            model_fit.predict(
                start=1, dynamic=self.sarima_params.predictor_params.dynamic_check
            )
        )

        # reverse log transformation for in-sample values
        if self.sarima_params.predictor_params.natural_log:
            in_samples = np.exp(in_samples)

        # combine residuals and is-samples to as part of one dataframe
        in_samps_residuals = pd.concat([residuals, in_samples], axis=1)
        in_samps_residuals.columns = ["Residuals", "In-Samples"]

        # make out-of-sample forecasts
        forecasts = model_fit.forecast(
            steps=self.sarima_params.predictor_params.number_of_forecasts
        ).to_frame(name="Forecasts")

        # reverse log transformation for forecasts
        if self.sarima_params.predictor_params.natural_log:
            forecasts = np.exp(forecasts)

        # populate model coefficients
        model_summary = self.model_summary(model_fit)

        return (
            knext.Table.from_pandas(forecasts),
            knext.Table.from_pandas(in_samps_residuals),
            knext.Table.from_pandas(model_summary),
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
