#!/usr/bin/env python3
import argparse
import aiohttp
from typing import Optional
from datetime import datetime
from influxdb import InfluxDBClient
from pydantic import BaseModel
from nordpool.elspot import AioPrices
import sys
import yaml
import asyncio
import logging

# Setup logging to console
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# Setup logging to stdout
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
logger.addHandler(stdout_handler)

API_URL = "http://{host}/api/v1/data"

class AppArgs(BaseModel):
    config_file: str
    dry_run: bool = False

class InfluxDBConfig(BaseModel):
    host: str
    port = 8086
    database = "energy"
    retention_policy: Optional[str]

    
class NordpoolConfig(BaseModel):
    areas: list[str]
    currency: str


class CheckerConfig(BaseModel):
    influxdb: InfluxDBConfig
    nordpool: NordpoolConfig

class HourlyPrice(BaseModel):
    start: datetime
    value: float

    class Config:
        allow_extra = True

class AreaValues(BaseModel):
    values: list[HourlyPrice]

    class Config:
        allow_extra = True


class AreaPrices(BaseModel):
    areas: dict[str, AreaValues]

    class Config:
        allow_extra = True

def _convert_price_to_cents_with_vat24(price: float) -> float:
    return (1.24 * price * 100) / 1000

async def collect_data(
    nordpool_config: NordpoolConfig,
    influx: InfluxDBClient,
    influx_config: InfluxDBConfig,
    dry_run: bool = False,
) -> None:
    # Get data
    client = aiohttp.client.ClientSession()
    prices = AioPrices(currency=nordpool_config.currency,  client=client)
    data = await prices.fetch(resolution=15, areas=nordpool_config.areas)
    area_prices = AreaPrices.parse_obj(data)
    logging.debug(area_prices)


    # Create json body
    json_body = [
        {
            "measurement": "nordpool_price",
            "tags": {
                "area": area,
            },
            "time": data.start.isoformat(),
            "fields": {"price": _convert_price_to_cents_with_vat24(data.value)},
        } for area, price_data in area_prices.areas.items() for data in price_data.values
    ]

    # Write to influxdb, ignore errors
    if dry_run:
        logging.debug("Send data (dry-run): %s", json_body)
    else:
        influx.write_points(json_body, retention_policy=influx_config.retention_policy)
    await client.close()


async def run(args: AppArgs) -> None:
    # Read config file
    with open(args.config_file, "r") as f:
        config = yaml.safe_load(f)
    checker_config = CheckerConfig(**config)

    # Connect to influxdb
    influx_config = checker_config.influxdb
    nordpool_config = checker_config.nordpool
    client_params = {
        "host": influx_config.host,
        "port": influx_config.port,
        "database": influx_config.database,
    }
    influx = InfluxDBClient(**client_params)

    await collect_data(nordpool_config, influx, influx_config, dry_run=args.dry_run)

def parse_args() -> AppArgs:
    parser = argparse.ArgumentParser(description='Script to collect data and write to InfluxDB.')
    parser.add_argument('config_file', type=str, help='Path to the configuration file.')
    parser.add_argument('--dry-run', action='store_true', help='Run the script in dry run mode.')

    args = parser.parse_args()
    return AppArgs(config_file=args.config_file, dry_run=args.dry_run)


def main() -> None:
    app_args = parse_args()
    asyncio.run(run(app_args))


if __name__ == "__main__":
    main()
