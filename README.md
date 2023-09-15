# Time Series Analysis Extension for KNIME

This repository contains code of the Time Series Analysis extension for [KNIME Analytics Platform](https://www.knime.com/knime-analytics-platform "KNIME"). This extension provides nodes for time series modelling, analysis and processing.
<br>
[Add screenshot of the workflow]
<br>
[Add screenshot of the ACF & PACF plots]
<br>
<br>
This extension is developed by Ali Asghar Marvi from the evangelism team at [KNIME](https://www.knime.com/ "KNIME"). The goal of the project is to utilize the bundled python packages that are shipped with KNIME and harness its functionalities and incorporate it within native KNIME nodes especially for Time Series Analysis. 

<br>

This extension is based on [Pandas](https://pandas.pydata.org/ "Pandas") datetime functionality and [Statsmodels](https://www.statsmodels.org/stable/index.html "Statsmodels") library. The supported data types for timestamps are defined in the [KNIME-Core](https://github.com/knime/knime-core/tree/master/org.knime.core/src/eclipse/org/knime/core/data/date "KNIME Date Types").

<br>

# Installation

This extension is not available on the KNIME's update site yet. However, it can be manually installed from the "Bundled extensions" directory by locally creating the update site to the zip file of the extension.
<br>
[Create a guide for installing extension with snapshots]

## Package Organization


* `scripts`: This directory consists of the source code for all the nodes of KNIME Time Series Extension.
* `Bundled extensions`: Contains zipped files of all the releases of the extension.
* `icons`: All the icons used for the nodes, categories and extension
* `bundling_ymls`: `.yml` files for each operating system.
* `tests`: Testflows for each node



