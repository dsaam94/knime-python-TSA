from .parent import _LearnerParams, _PredictorParams
import knime.extension as knext
import pickle


@knext.parameter_group(label="Sarima Model Parameters")
class SLearnerParams(_LearnerParams):
    """

    Sarima Learner Parameter class inheriting the private class _LearnerParams above.

    The learner attributes are put together in the parameter group called "Sarima Model Parameters".

    """

    def __init__(self):
        super().__init__()


@knext.parameter_group(label="Sarima Predictor Parameters")
class SPredictorParams(_PredictorParams):

    """

    SARIMA Predictor Parameter class inheriting the private class _PredictorParams above.

    The predictor attributes are put together in the parameter group called "Sarima Model Parameters".

    """

    def __init__(self):
        super().__init__()


@knext.parameter_group(label="Sarima Forecaster Parameters")
class SarimaForecasterParms:
    """

    SARIMA Forecaster nodes parameters put together in one complete group called "Sarima Forecaster Parameters"

    """

    learner_params = SLearnerParams()
    predictor_params = SPredictorParams()
