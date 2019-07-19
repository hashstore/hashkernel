# Etnry point for mypy to evaluate all modules
def list_all_prod_modules():
    """
    >>> list_all_prod_modules()
    import hashkernel
    import hashkernel.auto_wire
    import hashkernel.base_x
    import hashkernel.docs
    import hashkernel.executible
    import hashkernel.file_types
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
        print(f"import {m}")
        for n in sorted(listdir(m.replace(".", "/"))):
            if n != "__init__.py" and n.endswith(".py"):
                print(f"import {m}.{n[:-3]}")


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
import hashkernel.docs
import hashkernel.executible
import hashkernel.file_types
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
