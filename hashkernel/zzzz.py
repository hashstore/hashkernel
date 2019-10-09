# Etnry point for mypy to evaluate all modules
def all_prod_modules():
    """
    >>> for m in all_prod_modules(): print(f'import {m}')
    import hashkernel
    import hashkernel.auto_wire
    import hashkernel.base_x
    import hashkernel.cli
    import hashkernel.docs
    import hashkernel.executible
    import hashkernel.file_types
    import hashkernel.files
    import hashkernel.hashing
    import hashkernel.kernel
    import hashkernel.log_box
    import hashkernel.logic
    import hashkernel.otable
    import hashkernel.packer
    import hashkernel.plugins
    import hashkernel.smattr
    import hashkernel.time
    import hashkernel.typings
    import hashkernel.zzzz
    import hashkernel.bakery
    import hashkernel.bakery.aio_client
    import hashkernel.bakery.aio_server
    import hashkernel.bakery.cask
    import hashkernel.bakery.kernel
    import hashkernel.bakery.msg_server
    import hashkernel.bakery.path
    import hashkernel.bakery.rack
    """
    from setuptools import find_packages
    from os import listdir

    for m in find_packages(exclude=("*.tests",)):
        yield m
        for n in sorted(listdir(m.replace(".", "/"))):
            if n != "__init__.py" and n.endswith(".py"):
                yield f"{m}.{n[:-3]}"


# keep in sync with output above
import hashkernel
import hashkernel.auto_wire
import hashkernel.bakery
import hashkernel.bakery.aio_client
import hashkernel.bakery.aio_server
import hashkernel.bakery.cask
import hashkernel.bakery.kernel
import hashkernel.bakery.msg_server
import hashkernel.bakery.path
import hashkernel.bakery.rack
import hashkernel.base_x
import hashkernel.cli
import hashkernel.docs
import hashkernel.executible
import hashkernel.file_types
import hashkernel.files
import hashkernel.hashing
import hashkernel.kernel
import hashkernel.log_box
import hashkernel.logic
import hashkernel.otable
import hashkernel.packer
import hashkernel.plugins
import hashkernel.smattr
import hashkernel.time
import hashkernel.typings
import hashkernel.zzzz
