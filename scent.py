import os
import sys

from sniffer.api import file_validator, runnable


@file_validator
def py_files(filename):
    return (
        filename.endswith(".py")
        or filename.endswith(".yaml")
        or filename.endswith(".rst")
    )


# run_envs = ["hk36", "hk37", "hk38"]
run_envs = ["hk36"]


def os_system_in_env(e, cmd):
    cmd = cmd if e == "current" else f". activate {e}; {cmd}"
    # print(f"> {cmd}")
    return os.system(cmd)


def run_tests(include_slow, envs, html=False):
    args = "" if include_slow else '-m "not slow"'
    cmd = f"python -m coverage run -p -m pytest {args}"
    env_states = [0 == os_system_in_env(e, cmd) for e in envs]
    print(dict(zip(envs, env_states)))
    cleanup_cmds = ["python -m coverage combine", "python -m coverage report -m"]
    if html:
        cleanup_cmds.append("python -m coverage html")
    for c in cleanup_cmds:
        os_system_in_env(envs[0], c)
    try:
        os.unlink(".coverage")
    except:
        pass
    return all(env_states)


"""
Tests to add:


"""


@runnable
def execute_some_tests(*args):
    # return run_tests(True, run_envs[1:], html=True)
    return run_tests(False, run_envs, html=True)


if __name__ == "__main__":
    envs = run_envs
    cmd = "test"
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if len(sys.argv) > 2:
            envs = sys.argv[2:]
    if cmd == "test":
        if not (run_tests(True, envs, html=True)):
            raise SystemExit(-1)
    elif cmd == "cleanup_envs":
        for env in envs:
            os.system(f"conda env remove -y -n {env}")
    elif cmd == "setup_envs":
        for env in envs:
            ver = ".".join(env[-2:])
            os.system(f"conda create -y -n {env} python={ver}")
            os.system(f". activate {env}; pip install -e .[dev]")
