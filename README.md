# Streaming-Analytics


## Summary

*Streaming-Analytics* is a small Spring-Boot-Application that periodically reads JSON messages from a RabbitMQ queue and generates descriptive statistical information about the values ​​found: mean, median, mode, standard deviation, quartiles, maximum value and minimum value.

The *JSON messages* in the input queue must have the format defined:
{
  "version": "1.0.0",
  "datastreams": [
    {
      "id": "temperature",
      "feed": "feed_t",
      "datapoints": [
        {
          "value": 18.5
        }
      ]
    }
  ],
  "device": "asset1"
}

The *result of the statistical calculation* will be saved, in JSON format with a structure in MongoDB database. 
{
  "temperature": {
    "device": "asset2",
    "data": {
      "$date": "2022-04-03T15:14:03.175Z"
    },
    "values": {
        "media": 9.5,
        "mode": 9.5,
        "max": 10,
        "min": 5
    }
  }
}