from .parent import _LearnerParams, _PredictorParams
import knime.extension as knext
import pickle
from util import utils as kutil


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


@knext.parameter_group(label="Settings")
class SarimaForecasterParms:
    """

    SARIMA settings to configure the parameters for the model.

    """

    # target column for modelling
    input_column = knext.ColumnParameter(
        label="Target Column",
        description="Table containing training data for fitting the SARIMA model, must contain a numeric target column with no missing values to be used for forecasting.",
        port_index=0,
        column_filter=kutil.is_numeric,
    )

    learner_params = SLearnerParams()
    predictor_params = SPredictorParams()
