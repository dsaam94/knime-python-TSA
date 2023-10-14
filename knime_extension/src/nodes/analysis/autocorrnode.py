import logging
import knime_extension as knext
from utils import knutils as kutil
import pandas as pd
import numpy as np
from ..configs.analysis.autocorr import AutocorrParams
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.graphics.tsaplots import plot_pacf
from statsmodels.tsa.stattools import acf, pacf
import matplotlib.pyplot as plt


LOGGER = logging.getLogger(__name__)

__category = knext.category(
    path="/community/ts",
    level_id="analysis",
    name="Analysis",
    description="Nodes for analysis for time series application",
    # starting at the root folder of the extension_module parameter in the knime.yml file
    icon="icons/icon.png"
)

@knext.node(
    name="Autocorrelation Analysis",
    node_type=knext.NodeType.VISUALIZER,
    icon_path="icons/icon.png",
    category=__category,
    id = "autocorrelation_analysis"
)
@knext.input_table(
    name="Input Table",
    description="Table with exogenous variable to inspect."
)

@knext.output_table(
    name="Lags & Correlation Values", 
    description="Aggregated output with modified date&time and selected granularity."
)

@knext.output_view(
    name="ACF & PACF Plot",
    description="Plot for identifying best parameters for (seosonality, q)."
)

class AutoCorrNode:


    analysisParams = AutocorrParams()



    def configure(self, configure_context:knext.ConfigurationContext, input_schema):
       
       self.analysisParams.target_col = kutil.column_exists_or_preset(
            configure_context, self.analysisParams.target_col, input_schema, kutil.is_numeric
        )
       
       return None
        
    

    def execute(self, exec_context: knext.ExecutionContext, input_table):

        # height and width of the autocorrelation plot
        __width = 12
        __height = 12

        #number of rows in the plot
        __nrows = 2

        # number of columns for plot
        __ncols = 1
       
        #confidence interval
        __alpha = 0.05

        df = input_table.to_pandas()

        regression_target= df[self.analysisParams.target_col] 
        self._exec_validate(regression_target)

        # compute acf values along with confidence intervals
        acf_x, acf_confint = acf(regression_target, nlags=self.analysisParams.max_lag, alpha = __alpha)

        margin_error_acf = 0.5*(acf_confint[:,1]- acf_confint[:,0])

        margin_error_acf = pd.DataFrame(margin_error_acf, columns = ["Margin of Error (ACF)"])

        acf_x = pd.DataFrame(acf_x, columns=["ACF"])

        # compute pacf values along with confidence intervals
        pacf_x, pacf_confint = pacf(regression_target, nlags=self.analysisParams.max_lag, alpha = __alpha)

        margin_error_pacf = 0.5*(pacf_confint[:,1]- pacf_confint[:,0])

        margin_error_pacf = pd.DataFrame(margin_error_pacf, columns = ["Margin of Error (PACF)"])

        pacf_x = pd.DataFrame(pacf_x, columns=["PACF"])

        df_out = pd.concat([acf_x, margin_error_acf, pacf_x, margin_error_pacf], axis=1).reset_index().rename(columns={"index":'Lags'})

        fig, ax = plt.subplots(nrows=__nrows, ncols=__ncols, figsize=(__width, __height))
        plot_acf(regression_target,lags=self.analysisParams.max_lag, ax=ax[0], alpha = __alpha)
        plot_pacf(regression_target,lags=self.analysisParams.max_lag, ax=ax[1], method='ols', alpha = __alpha)

        plt.tight_layout()
        plt.show()

        return  (knext.Table.from_pandas(df_out), knext.view_matplotlib())


    def _exec_validate(self, target):
        """
        This function validates target regression column at Pandas end
        """
       ######################################################## 
       # TARGET COLUMN CHECK 
       ########################################################
       
        # check for missing values first
        if(kutil.check_missing_values(target)):
            missing_count = kutil.count_missing_values(target)
            raise knext.InvalidParametersError(
                f"""There are "{missing_count}" number of missing values in the target column."""
            )
        
        # check maximum lags cannot be more than the number of rows
        if(self.analysisParams.max_lag >= kutil.number_of_rows(target)):

            raise knext.InvalidParametersError(
                f"Maximum number of lags cannot be greater than or equal to the number of rows"
            )