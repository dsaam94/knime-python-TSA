import knime_extension as knext
import logging
LOGGER = logging.getLogger(__name__)


category = knext.category(
    path="/community",
    level_id="ts",
    name="Time Series Analysis",
    description="Python Nodes for Time Series Analysis",
    # starting at the root folder of the extension_module parameter in the knime.yml file
    icon="icons/icon.png",
)

import models
import processing
import visuals

LOGGER.warn(processing)
LOGGER.warn(visuals)
LOGGER.warn(models)

