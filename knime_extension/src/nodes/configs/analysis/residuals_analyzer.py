import logging
import knime_extension as knext
from util import utils as kutil

LOGGER = logging.getLogger(__name__)


class ResidualAnalyzerParams:

    residuals_col = knext.ColumnParameter(
        label="Residuals",
        description="The numeric value column to analyze residuals from the model.",
        port_index=0,
        column_filter=kutil.is_numeric,
    )

    opacity = knext.DoubleParameter(
        label="Opacity",
        description="Regulate transparency of the points on the scatter plot",
        default_value=0.7,
        min_value=0.1,
        max_value=1.0,
    )

    kde = knext.BoolParameter(
        label="Kernel Density Estimator",
        description="Check this option to compute a kernel density estimate for smoothing the distribution and visualize it on the plot.",
        default_value=True,
    )
