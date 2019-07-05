import pkg_resources


def query_plugins(cls: type, ep_name: str):
    return filter(lambda v: issubclass(type(v), cls), load_plugins(ep_name))


def load_plugins(ep_name: str):
    return (ep.load() for ep in pkg_resources.iter_entry_points(ep_name))
