import logging
import knime_extension as knext
from util import utils as kutil
from ..configs.analysis.residuals_analyzer import ResidualAnalyzerParams
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as tick
import seaborn as sns
from scipy import stats
from scipy.stats import skew, kurtosis
from statsmodels.stats.stattools import durbin_watson


LOGGER = logging.getLogger(__name__)


@knext.node(
    name="Residual Analyzer",
    node_type=knext.NodeType.VISUALIZER,
    icon_path="icons/Analysis/Residual_Analyzer.png",
    category=kutil.category_analytics,
    id="residual_analyzer",
)
@knext.input_table(
    name="Residuals",
    description="The input table that contains a numeric column representing the residuals from a forecasting model.",
)
@knext.output_table(
    name="Cummulative Sum or Residuals & Variance",
    description="Table containing the values from the Cumulative sums plot",
)
@knext.output_table(
    name="Summary Statistics",
    description="Table representation of the test statistics",
)
@knext.output_view(
    name="Residual Analysis",
    description="Plots for investigating residuals.",
)
class ResidualAnalyzerNode:
    """
    This node analyzes the residuals of a SARIMA model.

    This node is designed to provide an overview of the residuals from a forecasting model. Upon inputting residual data, the node processes and visualizes the information through a multi-part dashboard with the following elements

    - **Residuals Scatter Plot**: This plot displays the residuals by observation index to help visually assess the randomness and identify patterns or systematic deviations, which are crucial for diagnosing model fit.
    - **Histogram of Residuals:** The histogram presents the distribution of residuals. A bell-shaped, symmetrical distribution centered around zero indicates that the residuals are normally distributed, an assumption underlying many models.
    - **Cumulative Sums Plot:** This plot features two lines; one showing the cumulative sum of residuals and the other displaying the cumulative sum of squared residuals, which serves as a proxy for variance. This dual-axis plot helps detect any shifts in the central tendency and changes in the variance of the residuals over time. The sum is expected to fluctuate around zero and the sum of squares is expected to grow linearly.
    - **Statistical Tests**:

        - **Normality Test (Shapiro-Wilk):** This test examines whether the residuals follow a normal distribution, reporting a test statistic and p-value. A non-normal distribution may suggest that the model does not fully capture the data’s behavior. The Test statistic ranges from 0 to 1 with 1 being a perfect match to the normal distribution.
        - **Autocorrelation Test (Durbin-Watson):** This test looks for the presence of autocorrelation in the residuals. A durbin-watson test statistic near 2 implies no autocorrelation while values farther from 2 indicate positive or negative autocorrelation in the materials suggesting some patterns in the data may not have been captured by the model.
    """

    residuals_col = ResidualAnalyzerParams.residuals_col
    opacity = ResidualAnalyzerParams.opacity
    kde = ResidualAnalyzerParams.kde

    def configure(
        self, configure_context: knext.ConfigurationContext, input_schema: knext.Schema
    ):

        self.residuals_col = kutil.column_exists_or_preset(
            configure_context,
            self.residuals_col,
            input_schema,
            kutil.is_numeric,
        )

        output_schema_1 = knext.Schema(
            [
                knext.double(),
                knext.double(),
            ],
            ["Cumulative Sum of Residuals", "Cumulative Sum of Variance"],
        )

        summary_schema = knext.Column(knext.double(), "values")
        return (output_schema_1, summary_schema)

    def execute(self, exec_context: knext.ExecutionContext, input_1: knext.Table):
        df_res = input_1.to_pandas()
        # Calculate Summary and Test Statistics
        summary_statistics = df_res[self.residuals_col].describe()

        skewness = skew(df_res[self.residuals_col].dropna())
        kurtosis_value = kurtosis(df_res[self.residuals_col].dropna(), fisher=False)

        # Perform Tests
        normality_test = stats.shapiro(df_res[self.residuals_col].dropna())
        dw_test = durbin_watson(df_res[self.residuals_col].dropna())

        # Set style
        sns.set_context("talk")

        # Creating composite view
        fig, axs = plt.subplots(2, 2, figsize=(15, 15))
        fig.subplots_adjust(top=0.9, hspace=0.3)

        # Main Title and Subtitle
        main_title = "Residuals Analysis"
        subtitle = "Visual and Statistical Overview of Model Residuals"
        fig.suptitle(main_title, fontsize=32, fontweight="bold")
        fig.text(0.5, 0.94, subtitle, fontsize=18, ha="center")

        # Residuals Plot
        axs[0, 0].scatter(
            x=range(len(df_res[self.residuals_col])),
            y=df_res[self.residuals_col],
            alpha=self.opacity,
        )
        axs[0, 0].axhline(y=0, color="r", linestyle="-", linewidth=2)
        axs[0, 0].set_title("Residuals Plot", fontsize=20, fontweight="bold")
        axs[0, 0].set_xlabel("Observation", fontsize=16, fontweight="bold")
        axs[0, 0].set_ylabel("Residuals", fontsize=16, fontweight="bold")

        # Histogram Plot
        sns.histplot(
            df_res[self.residuals_col],
            kde=self.kde,
            ax=axs[0, 1],
            edgecolor="black",
            linewidth=1.25,
        )
        axs[0, 1].set_title("Histogram of Residuals", fontsize=20, fontweight="bold")
        axs[0, 1].set_xlabel("Residuals", fontsize=16, fontweight="bold")
        axs[0, 1].set_ylabel("Frequency", fontsize=16, fontweight="bold")

        # Cumulative Sum Residuals Plot
        axs[1, 0].plot(
            range(len(df_res[self.residuals_col])),
            (df_res[self.residuals_col].cumsum()),
        )
        axs[1, 0].set_title("Cumulative Sums", fontsize=20, fontweight="bold")
        axs[1, 0].set_xlabel("Observation", fontsize=16, fontweight="bold")
        axs[1, 0].set_ylabel(
            "Cumulative Sum of Residuals",
            color="#1f77b4",
            fontsize=16,
            fontweight="bold",
        )
        axs[1, 0].xaxis.set_major_locator(tick.MaxNLocator(integer=True, nbins=6))

        # Cumulative Sum of Variance Plot as Secondary Axis
        ax2 = axs[1, 0].twinx()
        ax2.plot(
            range(len(df_res[self.residuals_col])),
            ((df_res[self.residuals_col] ** 2).cumsum()),
            color="green",
            label="Cumulative Sum of Variance",
        )
        ax2.set_ylabel(
            "Cumulative Sum of Variance", color="green", fontsize=16, fontweight="bold"
        )
        ax2.tick_params(axis="y")
        ax2.grid(False)

        # Textual Information (Summary Statistics, Metrics, Tests)
        text_str = "\n".join(
            (
                f"Summary Statistics:",
                f"Mean: {summary_statistics['mean']:.2f}",
                f"Std: {summary_statistics['std']:.2f}",
                f"Min: {summary_statistics['min']:.2f}",
                f"Median: {summary_statistics['50%']:.2f}",
                f"Max: {summary_statistics['max']:.2f}",
                f"Skewness: {skewness:.2f}",
                f"Kurtosis: {kurtosis_value:.2f}",
                "",
                f"Normality Test (Shapiro-Wilk):",
                f"Statistic: {normality_test[0]:.4f}, p-value: {normality_test[1]:.4g}",
                "",
                f"Autocorrelation Test (Durbin-Watson):",
                f"Statistic: {dw_test:.4f}",
            )
        )

        # Using the fourth plot area for text
        axs[1, 1].set_title(
            "Test Metrics and Statistics", fontsize=20, fontweight="bold"
        )
        axs[1, 1].axis("off")
        axs[1, 1].text(
            0.5,
            0.5,
            text_str,
            fontsize=18,
            ha="center",
            va="center",
            transform=axs[1, 1].transAxes,
        )

        df_out = pd.concat(
            [
                pd.DataFrame(df_res[self.residuals_col].cumsum()),
                pd.DataFrame((df_res[self.residuals_col] ** 2).cumsum()),
            ],
            axis=1,
        )
        df_out.columns = ["Cumulative Sum of Residuals", "Cumulative Sum of Variance"]

        rename_col = "values"
        df_stats = pd.concat(
            [
                summary_statistics.to_frame().rename(
                    columns={self.residuals_col: rename_col}
                ),
                pd.DataFrame(
                    skewness,
                    index=["Skewness"],
                    columns=[rename_col],
                ),
                pd.DataFrame(
                    kurtosis_value,
                    index=["Kurtosis"],
                    columns=[rename_col],
                ),
                pd.DataFrame(
                    normality_test[0],
                    index=["Normality Test (Shapiro-Wilk) Statistics"],
                    columns=[rename_col],
                ),
                pd.DataFrame(
                    normality_test[1],
                    index=["Normality Test (Shapiro-Wilk) p-value"],
                    columns=[rename_col],
                ),
                pd.DataFrame(
                    dw_test,
                    index=["Autocorrelation Test (Durbin-Watson) Statistics"],
                    columns=[rename_col],
                ),
            ]
        )

        # df_stats.columns = ["Summary Statistics"]
        plt.tight_layout()
        plt.show()

        return (
            knext.Table.from_pandas(df_out),
            knext.Table.from_pandas(df_stats),
            knext.view_matplotlib(),
        )
