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

        return forecast_schema

    def execute(self, exec_context: knext.ExecutionContext, input_1, input_2):
        exog_df = input_2.to_pandas()

        exog_var_forecasts = exog_df[
            self.sarimax_params.predictor_params.exog_column_forecasts
        ]

        model_fit = pickle.loads(input_1)
        self._exec_validate(exog_var_forecasts)

        # make out-of-sample forecasts
        forecasts = model_fit.forecast(
            steps=self.sarimax_params.predictor_params.number_of_forecasts,
            exog=exog_var_forecasts,
        ).to_frame(name="Forecasts")

        # reverse log transformation for forecasts
        if self.sarimax_params.predictor_params.natural_log:
            forecasts = np.exp(forecasts)

        return knext.Table.from_pandas(forecasts)

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
