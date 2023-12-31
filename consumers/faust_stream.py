"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# Define a Faust Stream that ingests data from the Kafka Connect stations topic and
# places it into a new topic with only the necessary information.
app = faust.App("station-stream", broker="kafka://localhost:9092", store="memory://")
# Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("com.udacity.connect.jdbc.stations", value_type=Station)
# Define the output Kafka Topic
out_topic = app.topic(
    "com.udacity.faust.streams.stations", partitions=1, value_type=TransformedStation
)
# Define a Faust Table
table = app.Table(
    name="transformed-stations",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


#
#
# Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def tranformStations(stations):
    async for station in stations:
        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=get_line(station),
        )


def get_line(station):
    if station.red:
        return "red"
    elif station.blue:
        return "blue"
    elif station.green:
        return "green"
    else:
        return "other"


if __name__ == "__main__":
    app.main()
