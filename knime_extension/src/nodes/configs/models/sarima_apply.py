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
        description="Optionally, use this option to reverse the transformation applied on the target column during the model training.",
        default_value=False,
    )


@knext.parameter_group(label="Sarima Forecaster (Apply) Settings")
class SarimaForecasterParms:
    """

    SARIMA Predictor settings to configure the predictions to be made by the model.

    """

    predictor_params = SPredictorApplyParams()
