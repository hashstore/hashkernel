# hashkernel

[![Build Status](https://dev.azure.com/sekash/Public/_apis/build/status/hashstore.hashkernel?branchName=master)](https://dev.azure.com/sekash/Public/_build/latest?definitionId=3&branchName=master)
[![pypi_version](https://img.shields.io/pypi/v/hashkernel.svg)](https://pypi.python.org/pypi/hashkernel)
[![pypi_support](https://img.shields.io/pypi/pyversions/hashkernel.svg)](https://pypi.python.org/pypi/hashkernel)

Kernel to build and run python code inside hashstore

## Installation

```shell
pip install hashkernel
```

## Setup dev environment with conda

```shell
#
# Cleanup old environments
. deactivate
conda env remove -n hk36
conda env remove -n hk37
#
# Create envs
conda create -y -n hk36 python=3.6
conda create -y -n hk37 python=3.7
. activate hk37
pip install -e .[dev]
. deactivate
. activate hk36
pip install -e .[dev]
#
#run smoke test watcher
sniffer
```

