import logging
import knime.extension as knext
from util import utils as kutil
from ..configs.models.sarima_apply import SarimaForecasterParms
import pandas as pd
import numpy as np
import pickle

LOGGER = logging.getLogger(__name__)


@knext.node(
    name="SARIMA Predictor",
    node_type=knext.NodeType.PREDICTOR,
    icon_path="icons/models/SARIMA_Forecaster-Apply.png",
    category=kutil.category_models,
    id="sarima_apply",
)
@knext.input_binary(
    name="Model Input",
    description="Trained SARIMA model",
    id="sarima.model",
)
@knext.output_table(
    name="Forecast",
    description="Table containing forecasts for the configured column, the first value will be one timestamp ahead of the final training value used.",
)
# @knext.output_table(
#     name="Residuals",
#     description="In-sample model prediction values and residuals i.e. difference between observed value and the predicted output.",
# )
# @knext.output_table(
#     name="Model Summary",
#     description="Table containing fitted model coefficients, variance of residuals (sigma2), and several model metrics along with their standard errors.",
# )
class SarimaForcasterApply:
    """
    This node generates forecasts with a (S)ARIMA Model.

    Based on a trained SARIMA model given at the model input port of this node, the forecasts values are computed.
    """

    sarima_params = SarimaForecasterParms()

    # merge in-samples and residuals (In-Samples & Residuals)
    def configure(self, configure_context, input_schema_1):
        if (
            self.sarima_params.predictor_params.natural_log
            and self.sarima_params.predictor_params.dynamic_check
        ):
            configure_context.set_warning(
                "Enabling dynamic predictions with log transformation can create invalid predictions."
            )

        forecast_schema = knext.Column(knext.double(), "Forecasts")
        # insamp_res_schema = knext.Schema(
        #     [knext.double(), knext.double()], ["Residuals", "In-Samples"]
        # )
        # model_summary_schema = knext.Column(knext.double(), "value")

        return forecast_schema
        # , insamp_res_schema, model_summary_schema)

    def execute(self, exec_context: knext.ExecutionContext, model_input):
        model_fit = pickle.loads(model_input)

        # # produce residuals
        # residuals = model_fit.resid

        # # in-samples
        # in_samples = pd.Series(dtype=np.float64)

        # in_samples = in_samples.append(
        #     model_fit.predict(
        #         start=1, dynamic=self.sarima_params.predictor_params.dynamic_check
        #     )
        # )

        # # reverse log transformation for in-sample values
        # if self.sarima_params.predictor_params.natural_log:
        #     in_samples = np.exp(in_samples)

        # # combine residuals and is-samples to as part of one dataframe
        # in_samps_residuals = pd.concat([residuals, in_samples], axis=1)
        # in_samps_residuals.columns = ["Residuals", "In-Samples"]

        # make out-of-sample forecasts
        forecasts = model_fit.forecast(
            steps=self.sarima_params.predictor_params.number_of_forecasts
        ).to_frame(name="Forecasts")

        # reverse log transformation for forecasts
        if self.sarima_params.predictor_params.natural_log:
            forecasts = np.exp(forecasts)

        # # populate model coefficients
        # model_summary = self.model_summary(model_fit)

        return (
            knext.Table.from_pandas(forecasts),
            # knext.Table.from_pandas(in_samps_residuals),
            # knext.Table.from_pandas(model_summary),
        )

    # def model_summary(self, model):
    #     # estimates of the parameter coefficients
    #     coeff = model.params.to_frame()

    #     # calculate standard deviation of the parameters in the coefficients
    #     coeff_errors = model.bse.to_frame().reset_index()
    #     coeff_errors["index"] = coeff_errors["index"].apply(lambda x: x + " Std. Err")
    #     coeff_errors = coeff_errors.set_index("index")

    #     # extract log likelihood of the trained model
    #     log_likelihood = pd.DataFrame(
    #         data=model.llf, index=["Log Likelihood"], columns=[0]
    #     )

    #     # extract AIC (Akaike Information Criterion)
    #     aic = pd.DataFrame(data=model.aic, index=["AIC"], columns=[0])

    #     # extract BIC (Bayesian Information Criterion)
    #     bic = pd.DataFrame(data=model.bic, index=["BIC"], columns=[0])

    #     # extract Mean Squared Error
    #     mse = pd.DataFrame(data=model.mse, index=["MSE"], columns=[0])

    #     # extract Mean Absolute error
    #     mae = pd.DataFrame(data=model.mae, index=["MAE"], columns=[0])

    #     summary = pd.concat(
    #         [coeff, coeff_errors, log_likelihood, aic, bic, mse, mae]
    #     ).rename(columns={0: "value"})

    #     return summary
