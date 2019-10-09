import aiohttp
import croniter
import dateutil
import nanotime
import pytz

modules = [nanotime, croniter, dateutil, aiohttp, pytz]


def main():
    print(f"cli here {modules}")


if __name__ == "__main__":
    main()
