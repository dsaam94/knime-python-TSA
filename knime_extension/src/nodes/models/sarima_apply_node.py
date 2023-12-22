import logging
import knime.extension as knext
from util import utils as kutil
from ..configs.models.sarima_apply import SPredictorApplyParams
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
class SarimaForcasterApply:
    """
    This node generates forecasts with a (S)ARIMA Model.

    Based on a trained SARIMA model given at the model input port of this node, the forecasts values are computed.
    """

    sarima_params = SPredictorApplyParams
    natural_log = sarima_params.natural_log
    dynamic_check = sarima_params.dynamic_check
    number_of_forecasts = sarima_params.number_of_forecasts

    # merge in-samples and residuals (In-Samples & Residuals)
    def configure(self, configure_context, input_schema_1):
        if self.natural_log and self.dynamic_check:
            configure_context.set_warning(
                "Enabling dynamic predictions with log transformation can create invalid predictions."
            )

        forecast_schema = knext.Column(knext.double(), "Forecasts")

        return forecast_schema

    def execute(self, exec_context: knext.ExecutionContext, model_input):
        model_fit = pickle.loads(model_input)

        # make out-of-sample forecasts
        forecasts = model_fit.forecast(steps=self.number_of_forecasts).to_frame(
            name="Forecasts"
        )

        # reverse log transformation for forecasts
        if self.natural_log:
            forecasts = np.exp(forecasts)

        return knext.Table.from_pandas(forecasts)
