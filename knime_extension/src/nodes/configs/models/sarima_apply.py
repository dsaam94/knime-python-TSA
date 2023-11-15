from .parent import _PredictorParams
import knime.extension as knext
import pickle


@knext.parameter_group(label="Sarima Predictor Parameters")
class SPredictorApplyParams(_PredictorParams):

    """

    SARIMA Predictor Parameter class inheriting the private class _PredictorParams above.

    The predictor attributes are put together in the parameter group called "Sarima Model Parameters".

    """

    natural_log = knext.BoolParameter(
        label="Reverse Log",
        description="Check this option only if the target column was logged during training the model.",
        default_value=False,
    )


@knext.parameter_group(label="Sarima Forecaster (Apply) Parameters")
class SarimaForecasterParms:
    """

    SARIMA Forecaster nodes parameters put together in one complete group called "Sarima Forecaster Parameters"

    """

    predictor_params = SPredictorApplyParams()
