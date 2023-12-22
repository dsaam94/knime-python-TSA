from .parent import _PredictorParams
import knime.extension as knext
import pickle


class SPredictorApplyParams(_PredictorParams):

    """

    SARIMA Predictor Parameter class inheriting the private class _PredictorParams above.

    The predictor attributes are put together in the parameter group called "Sarima Model Parameters".

    """

    natural_log = knext.BoolParameter(
        label="Reverse Log",
        description="Check this box if you applied the log transform inside the SARIMA Forecaster node while training your model. It will reverse the transform before generating forecasts.",
        default_value=False,
    )


class SarimaForecasterParms:
    """

    SARIMA Predictor settings to configure the forecasts generated by the input model.

    """

    predictor_params = SPredictorApplyParams()
