import knime.extension as knext


##################################################################

##################################################################


class _LearnerParams:

    """
    A protected class containing common parameters for both SARIMA and SARIMAX learner along with its validation functions.

    """

    ar_order_param = knext.IntParameter(
        label="AR Order (p)",
        description="The number of lagged observations to be used in the model. ",
        default_value=0,
        min_value=0,
    )

    i_order_param = knext.IntParameter(
        label="I Order (d)",
        description="The number of times to apply differencing before training the model.",
        default_value=0,
        min_value=0,
    )

    ma_order_param = knext.IntParameter(
        label="MA Order (q)",
        description="The number of lagged forecast errors to be used in the model.",
        default_value=0,
        min_value=0,
    )

    seasoanal_ar_order_param = knext.IntParameter(
        label="Seasonal AR Order (P)",
        description="The number of seasonally lagged observations to be used in the model.",
        default_value=0,
        min_value=0,
    )

    seasoanal_i_order_param = knext.IntParameter(
        label="Seasonal I Order (D)",
        description="The number of times to apply seasonal differencing before training the model.",
        default_value=0,
        min_value=0,
    )

    seasoanal_ma_order_param = knext.IntParameter(
        label="Seasonal MA Order (Q)",
        description="The number of seasonal lagged forecast errors to be used in the model. ",
        default_value=0,
        min_value=0,
    )

    seasonal_period_param = knext.IntParameter(
        label="Seasonal Period",
        description="Specify the length of the Seasonal Period",
        default_value=2,
        min_value=2,
    )

    natural_log = knext.BoolParameter(
        label="Log Transform",
        description="Optionally log your target column before model fitting and exponentiate the forecast before output. This may help reduce variance in the training data.",
        default_value=False,
    )

    # validation of parameters
    def validate(self, values):
        # seasonality provided as 1 or 0
        if values["seasonal_period_param"] < 2:
            raise knext.InvalidParametersError("Seasonality can not be less than 2.")

        # handle P >0 and p >= s
        if (values["seasoanal_ar_order_param"] > 0) and (
            values["ar_order_param"] >= values["seasonal_period_param"]
        ):
            raise knext.InvalidParametersError(
                "Autoregressive terms overlap with seasonal autogressive terms, p should be less than S when using seasonal auto regressive terms"
            )

        # handle Q > 0 and q >= s
        if (values["seasoanal_ma_order_param"] > 0) and (
            values["ma_order_param"] >= values["seasonal_period_param"]
        ):
            raise knext.InvalidParametersError(
                "Moving average terms overlap with seasonal moving average terms, q should be less than S when using seasonal moving average terms."
            )


##################################################################

##################################################################


class _PredictorParams:
    """
    A protected class containing common parameters for both SARIMA and SARIMAX predictors along with its validation functions.

    """

    number_of_forecasts = knext.IntParameter(
        label="Forecast",
        description="Forecasts of the given time series *h* period ahead of the training data.",
        default_value=1,
        min_value=1,
    )

    dynamic_check = knext.BoolParameter(
        label="Dynamic",
        description="Check this box to use in-sample prediction as lagged values. Otherwise use true values.",
        default_value=False,
    )

    def validate(self, values):
        if values["number_of_forecasts"] < 1:
            raise ValueError("At least one forecast should be made by the model.")
