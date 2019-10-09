from hashkernel.smattr import MoldConfig, SmAttr


class Codebase(SmAttr):
    __mold_config__ = MoldConfig(omit_optional_null=True)
    name: str
