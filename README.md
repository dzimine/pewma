Here I play with Probabalistic Exponentially Weighted Moving Average PEWMA algorithm,
starting from [this AWS blog](https://aws.amazon.com/blogs/iot/anomaly-detection-using-aws-iot-and-aws-lambda/)
but simplifying it for running at the edge/on Greengrass.

Usage:

```
$ python function.py | grep true | wc -l
    4654
$ python function.py | grep false | wc -l
   21541

```