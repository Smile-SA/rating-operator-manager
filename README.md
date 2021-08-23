# **rating-operator-manager**

**DISCLAIMER** This component is part of the **rating-operator** application and is not designed to work as a standalone program.

**DISCLAIMER** The *legacy* rating implementation provides means to connect to the **metering-operator** to extract and rates the Reports data.
The **metering-operator** being deprecated, this feature is not maintained anymore and will not be documented fully.

**Rating-operator-manager** watches and updates CustomResources, helping other components achieve their roles.
It plays a role in most of the features of the **rating-operator**, handling all event based callbacks.

## CustomResources

Below are described each CustomResource watched by the **rating-operator-manager**.

Each resource triggering callbacks when generated, modified or deleted.

### RatingRules

The **RatingRules** are one of the two base configurations of the **rating-operator**.
They are used for multiple purposes:

- Declaring list of rules, with or without labels, to be used in **promQL** (By [RatingRuleInstances](https://github.com/alterway/rating-operator/blob/master/documentation/CRD.md))
- Configure the metrics rated by the *legacy* version of the **rating-operator** (with metering-operator, deprecated)

More information on RatingRules can be found in [Custom Resource documentation](https://github.com/alterway/rating-operator/blob/master/documentation/CRD.md).

### RatedMetrics

The **RatedMetrics** are CustomResources that are generated or updated each time a metric is rated.
It helps other operators bind with the data, by subscribing to the events of the RatedMetrics.
They hold only two values:

- The name of the metric that have been rated
- The time at which it has been

More information on RatedMetrics can be found in [Custom Resource documentation](https://github.com/alterway/rating-operator/blob/master/documentation/CRD.md).

### Namespaces

This component also watch Namespaces, looking for annotations and labels, to match tenant with their respective namespaces.
It help replicating the user/namespaces associations in database if no external user management solution isn't provided.

## Usage

To learn more on how to configure and use the component, please refer to the [rating-operator documentation](https://github.com/alterway/rating-operator/blob/master/README.md).
