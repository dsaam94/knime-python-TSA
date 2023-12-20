import logging
import knime_extension as knext
from util import utils as kutil
import pandas as pd
import numpy as np
from ..configs.analysis.autocorr import AutocorrParams
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.graphics.tsaplots import plot_pacf
from statsmodels.tsa.stattools import acf, pacf
import matplotlib.pyplot as plt


LOGGER = logging.getLogger(__name__)


@knext.node(
    name="Autocorrelation Plot",
    node_type=knext.NodeType.VISUALIZER,
    icon_path="icons/Analysis/Autocorrelation_Analysis.png",
    category=kutil.category_analytics,
    id="autocorrelation_analysis",
)
@knext.input_table(
    name="Input Table", description="Table containing the numeric column to analyse."
)
@knext.output_table(
    name="Lags & Plot Values",
    description="Table representation of ACF and PACF plots.",
)
@knext.output_view(
    name="ACF & PACF Plot",
    description="Plots for investigating autocorrelations.",
)
class AutoCorrNode:
    """
    Create ACF and PACF diagnostic plots to visualize stationarity and autocorrelations in a time series column.

    This node will generate both an Autocorrelation Function (ACF) plot and a Partial Auotcorrelation Function (PACF) plot. The ACF plot can be used to visualize correlations between the time series and lagged copies of itself, use this to identify seasonalities in your data. The PACF plot is a modified version of the ACF that attempts to account for and remove serial correlation, use this to identify key lag values to include in (S)ARIMA models.
    """

    # analysis_params = AutocorrParams()

    target_col = AutocorrParams.target_col
    max_lag = AutocorrParams.max_lag

    def configure(self, configure_context: knext.ConfigurationContext, input_schema):
        self.target_col = kutil.column_exists_or_preset(
            configure_context,
            self.target_col,
            input_schema,
            kutil.is_numeric,
        )

        output_schema = knext.Schema(
            [
                knext.int32(),
                knext.double(),
                knext.double(),
                knext.double(),
                knext.double(),
            ],
            ["Lags", "ACF", "Margin of Error (ACF)", "PACF", "Margin of Error (PACF)"],
        )

        return output_schema

    def execute(self, exec_context: knext.ExecutionContext, input_table):
        # height and width of the autocorrelation plot
        __width = 12
        __height = 12

        # number of rows in the plot
        __nrows = 2

        # number of columns for plot
        __ncols = 1

        # confidence interval
        __alpha = 0.05

        df = input_table.to_pandas()

        regression_target = df[self.target_col]
        self._exec_validate(regression_target)
        exec_context.set_progress(0.1)
        # compute acf values along with confidence intervals
        acf_x, acf_confint = acf(regression_target, nlags=self.max_lag, alpha=__alpha)

        # compute mid-point of upper and lower bounds for acf
        margin_error_acf = 0.5 * (acf_confint[:, 1] - acf_confint[:, 0])

        margin_error_acf = pd.DataFrame(
            margin_error_acf, columns=["Margin of Error (ACF)"]
        )

        acf_x = pd.DataFrame(acf_x, columns=["ACF"])
        exec_context.set_progress(0.2)
        # compute pacf values along with confidence intervals
        pacf_x, pacf_confint = pacf(
            regression_target, nlags=self.max_lag, alpha=__alpha
        )
        # compute mid-point of upper and lower bounds for pacf
        margin_error_pacf = 0.5 * (pacf_confint[:, 1] - pacf_confint[:, 0])

        margin_error_pacf = pd.DataFrame(
            margin_error_pacf, columns=["Margin of Error (PACF)"]
        )
        exec_context.set_progress(0.3)
        pacf_x = pd.DataFrame(pacf_x, columns=["PACF"])

        df_out = (
            pd.concat([acf_x, margin_error_acf, pacf_x, margin_error_pacf], axis=1)
            .reset_index()
            .rename(columns={"index": "Lags"})
        )
        df_out["Lags"] = df_out["Lags"].astype(np.int32)

        exec_context.set_progress(0.4)

        _, ax = plt.subplots(nrows=__nrows, ncols=__ncols, figsize=(__width, __height))
        plot_acf(
            regression_target,
            lags=self.max_lag,
            ax=ax[0],
            alpha=__alpha,
        )
        exec_context.set_progress(0.5)
        plot_pacf(
            regression_target,
            lags=self.max_lag,
            ax=ax[1],
            method="ols",
            alpha=__alpha,
        )

        exec_context.set_progress(0.6)
        plt.tight_layout()
        plt.show()
        exec_context.set_progress(0.9)

        return (knext.Table.from_pandas(df_out), knext.view_matplotlib())

    def _exec_validate(self, target):
        """
        This function validates selected numeric column at Panda's end
        """
        ########################################################
        # TARGET COLUMN CHECK
        ########################################################

        # check for missing values first
        if kutil.check_missing_values(target):
            missing_count = kutil.count_missing_values(target)
            raise knext.InvalidParametersError(
                f"""There are {missing_count} missing values in the selected column. Consider using a Missing Value node to impute missing values beforehand."""
            )

        # check maximum lags cannot be more than the number of rows
        if self.max_lag >= kutil.number_of_rows(target):
            raise knext.InvalidParametersError(
                "Maximum number of lags cannot be greater than or equal to the number of rows."
            )
