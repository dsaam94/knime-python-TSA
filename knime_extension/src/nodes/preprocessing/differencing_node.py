import logging
import knime.extension as knext
from util import utils as kutil
import pandas as pd
import numpy as np
from ..configs.preprocessing.difference import SeasonalDifferencingParams

LOGGER = logging.getLogger(__name__)


@knext.node(
    name="Differencer",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/preprocessing/Differencing.png",
    category=kutil.category_processsing,
    id="differencing",
)
@knext.input_table(
    name="Input Data",
    description="Table containing the numeric column to apply differencing.",
)
@knext.output_table(
    name="Input Data with Differenced Column",
    description="Output table containing the differenced column.",
)
class SeasonalDifferencingNode:
    """
    Differences a Column by subtracting from each row the value of a prior row.

    Select a lag value for differencing to represent the number of rows back to use when applying the differencing calculation. For example a lag value of 1 will subtract from each row the value of the previous row. A value of 2 will subtract from each row the value of the row before the previous. It is based on [Panda’s Series Differencing method](https://pandas.pydata.org/docs/reference/api/pandas.Series.diff.html).

    """

    diff_params = SeasonalDifferencingParams()

    def configure(
        self, configure_context: knext.ConfigurationContext, input_schema: knext.Schema
    ):
        self.diff_params.target_column = kutil.column_exists_or_preset(
            configure_context,
            self.diff_params.target_column,
            input_schema,
            kutil.is_numeric,
        )

        # get the data type of the selected column
        ktype = (
            input_schema[[self.diff_params.target_column]].delegate._columns[0].ktype
        )

        return input_schema.append(
            knext.Column(
                ktype,
                self.diff_params.target_column
                + "("
                + (str(-self.diff_params.lags))
                + ")",
            )
        )

    def execute(
        self, exec_context: knext.ExecutionContext, input_table
    ):  # NOSONAR exec_context is necessary
        df = input_table.to_pandas()

        target_col = df[self.diff_params.target_column]

        target_col_new = target_col.diff(periods=self.diff_params.lags)

        df[
            self.diff_params.target_column + "(" + (str(-self.diff_params.lags)) + ")"
        ] = target_col_new

        return knext.Table.from_pandas(df)
