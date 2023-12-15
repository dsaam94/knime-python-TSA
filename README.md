# Time Series Analysis Extension for KNIME

This repository contains code of the Time Series Analysis extension for [KNIME Analytics Platform](https://www.knime.com/knime-analytics-platform "KNIME"). This extension provides nodes for time series modeling, analysis, and processing.

This extension was developed by Ali Asghar Marvi & Corey Weisinger from the evangelism team at [KNIME](https://www.knime.com/ "KNIME"). The project's goal is to utilize the bundled Python packages shipped with KNIME, harness its functionalities, and incorporate it within native KNIME nodes for Time Series Analysis. 

This extension is based on [Pandas](https://pandas.pydata.org/ "Pandas") DateTime functionality and [Statsmodels](https://www.statsmodels.org/stable/index.html "Statsmodels") library. The supported data types for timestamps are defined in the [KNIME-Core](https://github.com/knime/knime-core/tree/master/org.knime.core/src/eclipse/org/knime/core/data/date "KNIME Date Types").


## Package Organization


* `knime_extension`: This directory contains the source code for all KNIME Time Series Extension nodes.
* `config.yml`: Example `config.yml` to point to the directory containing the source code of the extension. This directory must also contain `knime.yml` file.
