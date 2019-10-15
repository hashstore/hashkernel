import aiohttp
import croniter
import dateutil
import nanotime
import pytz
import sys

from hashkernel import GlobalRef
from hashkernel.file_types import guess_name
from hashkernel.logic import HashLogic

modules = [nanotime, croniter, dateutil, aiohttp, pytz]


def main():

    print(sys.argv)
    print(sys.path)
    print(guess_name('abc.hsb'))
    print(f"cli here {modules}")

    module_gref = GlobalRef(sys.argv[1])
    assert module_gref.module_only()
    hl = HashLogic.from_module(module_gref.get_module())
    print(str(hl))

if __name__ == "__main__":
    main()
