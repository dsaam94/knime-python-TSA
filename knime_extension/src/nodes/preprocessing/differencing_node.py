import logging
import knime.extension as knext
from util import utils as kutil
import pandas as pd
import numpy as np
from ..configs.preprocessing.difference import SeasonalDifferencingParams

LOGGER = logging.getLogger(__name__)

__category = knext.category(
    path="/community/ts",
    level_id="proc",
    name="Preprocessing",
    description="Nodes for pre-processing date&time.",
    # starting at the root folder of the extension_module parameter in the knime.yml file
    icon="icons/icon.png",
)


@knext.node(
    name="Differencing",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/icon.png",
    category=__category,
    id="differencing",
)
@knext.input_table(
    name="Input Data",
    description="Table contains the regression column to be diffeenced",
)
@knext.output_table(name="Aggregations", description="Output the column having ")
class SeasonalDifferencingNode:
    """
    yet to be done
    """

    diff_params = SeasonalDifferencingParams()

    def configure(self, configure_context: knext.ConfigurationContext, input_schema):
        self.diff_params.target_column = kutil.column_exists_or_preset(
            configure_context,
            self.diff_params.target_column,
            input_schema,
            kutil.is_numeric,
        )

        return None

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
