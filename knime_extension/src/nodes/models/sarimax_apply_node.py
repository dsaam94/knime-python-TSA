import logging
import knime.extension as knext
from util import utils as kutil
from ..configs.models.sarimax_apply import SarimaxForecasterParms
import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX
import pickle

LOGGER = logging.getLogger(__name__)


@knext.node(
    name="SARIMAX Predictor",
    node_type=knext.NodeType.LEARNER,
    icon_path="icons/models/SARIMAX_Forecaster-Apply.png",
    category=kutil.category_models,
    id="sarimax_apply",
)
@knext.input_binary(
    name="Input Data", description="Trained SARIMAX model.", id="sarimax.model"
)
@knext.input_table(name="Exogenous Input", description="Link to exogenous variable")
@knext.output_table(
    name="Forecast", description="Forecasted values and their standard errors"
)
# @knext.output_table(
#     name="Coefficients and Statistics", description="Residuals from the training model"
# )
# @knext.output_table(
#     name="Model Summary",
#     description="Table containing coefficient statistics and other criterion.",
# )
class SXForecaster:
    """
    This node  generates forecasts with a (S)ARIMAX Model.

    Based on a trained SARIMAX model given at the model input port of this node, the forecast values are computed. This apply node can also be used to update exogenous variable data for forecasting.
    """

    sarimax_params = SarimaxForecasterParms()

    def configure(
        self,
        configure_context: knext.ConfigurationContext,
        input_model,
        input_schema_2,
    ):
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
        # insamp_res_schema = knext.Schema(
        #     [knext.double(), knext.double()], ["Residuals", "In-Samples"]
        # )
        # model_summary_schema = knext.Column(knext.double(), "value")

        return (
            forecast_schema,
            # insamp_res_schema,
            # model_summary_schema,
        )

    def execute(self, exec_context: knext.ExecutionContext, input_1, input_2):
        exog_df = input_2.to_pandas()

        exog_var_forecasts = exog_df[
            self.sarimax_params.predictor_params.exog_column_forecasts
        ]

        model_fit = pickle.loads(input_1)
        self._exec_validate(exog_var_forecasts)

        # residuals = model_fit.resid

        # # in-samples
        # in_samples = pd.Series(dtype=np.float64)
        # in_samples = in_samples.append(
        #     model_fit.predict(
        #         start=1, dynamic=self.sarimax_params.predictor_params.dynamic_check
        #     )
        # )

        # # combine residuals and is-samples to as part of one dataframe
        # in_samps_residuals = pd.concat([residuals, in_samples], axis=1)
        # in_samps_residuals.columns = ["Residuals", "In-Samples"]

        # # reverse log transformation for in-sample values
        # if self.sarimax_params.predictor_params.natural_log:
        #     in_samples = np.exp(in_samples)

        # make out-of-sample forecasts
        forecasts = model_fit.forecast(
            steps=self.sarimax_params.predictor_params.number_of_forecasts,
            exog=exog_var_forecasts,
        ).to_frame(name="Forecasts")

        # reverse log transformation for forecasts
        if self.sarimax_params.predictor_params.natural_log:
            forecasts = np.exp(forecasts)

        # # populate model coefficients
        # model_summary = self.model_summary(model_fit)

        return (
            knext.Table.from_pandas(forecasts),
            # knext.Table.from_pandas(in_samps_residuals),
            # knext.Table.from_pandas(model_summary),
        )

    # function to perform validation on dataframe within execution context
    def _exec_validate(self, exog_forecast):
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
                "The number of forecasts should be equal to the length of the exogenous input for forecasts."
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
