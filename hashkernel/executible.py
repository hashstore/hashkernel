import inspect
import traceback
from typing import Any, Dict, Generator, Optional

from hashkernel import Conversion, GlobalRef, exception_message, to_json
from hashkernel.bakery import QuestionMsg, ResponseChain, ResponseMsg
from hashkernel.smattr import Mold, SmAttr, extract_molds_from_function

EDGE_CLS_NAMES = {"Input", "Output"}
IN_OUT = "in_out"
mold_name = lambda cls_name: f"{cls_name[:-3].lower()}_mold"
EDGE_MOLDS = set(mold_name(cls_name) for cls_name in EDGE_CLS_NAMES)
EXECUTIBLE_TYPE = "executible_type"


class ExecutibleFactory(type):
    def __init__(cls, name, bases, dct):
        defined_vars = set(dct)
        if not defined_vars.issuperset(EDGE_MOLDS):
            if IN_OUT in dct:
                InOut = dct[IN_OUT]
                for cls_name in EDGE_CLS_NAMES:
                    if cls_name in dct:
                        raise AssertionError(f"Ambiguous {cls_name} and {IN_OUT}")
                    mold_n = mold_name(cls_name)
                    if mold_n in dct:
                        raise AssertionError(f"Ambiguous {mold_n} and {IN_OUT}")
                    setattr(cls, mold_n, getattr(InOut, mold_n))
            else:
                for cls_name in EDGE_CLS_NAMES:
                    if cls_name in dct:
                        item_cls = dct[cls_name]
                        mold_n = mold_name(cls_name)
                        if mold_n in dct:
                            raise AssertionError(f"Ambiguous {mold_n} and {cls_name}")
                        setattr(cls, mold_n, Mold(item_cls))
        if any(not (hasattr(cls, s)) for s in EDGE_MOLDS):
            raise AttributeError(f"Undefined: {EDGE_MOLDS}")

    def exec_factory(cls) -> "Executible":
        if not hasattr(cls, EXECUTIBLE_TYPE):
            raise AttributeError(f"{EXECUTIBLE_TYPE} has to be defined")
        exec_cls = getattr(cls, EXECUTIBLE_TYPE)
        mold = Mold.ensure_it(exec_cls)
        return exec_cls(mold.pull_attrs(cls), ref=GlobalRef(cls))


class Executible(SmAttr):
    ref: GlobalRef
    in_mold: Mold
    out_mold: Mold

    @classmethod
    def ___factory__(cls):
        def build_it(o):
            if inspect.isfunction(o):
                return Function.parse(o)
            else:
                ref = GlobalRef.ensure_it(o["ref"])
                inst = ref.get_instance()
                if inspect.isfunction(inst):
                    return Function.parse(inst)
                if inspect.isclass(inst) and issubclass(inst, ExecutibleFactory):
                    return inst.exec_factory()
                raise AttributeError(f"Cannot build `Executible` out: {o}")

        return build_it

    def run(self, ctx: "ExecContext"):
        raise AssertionError("unimplemented")

    def invoke(self, input: Dict[str, Any]) -> Generator[Any, None, None]:
        ctx = ExecContext(
            exec=self,
            invocation=QuestionMsg(
                ref=self.ref, data=self.in_mold.mold_dict(input, Conversion.TO_JSON)
            ),
        )
        yield ctx.invocation
        try:
            self.run(ctx)
        except:
            ctx.final_state = ResponseMsg(
                data={"msg": exception_message()}, traceback=traceback.format_exc()
            )
        yield ctx.final_state


class ExecContext(SmAttr):
    exec: Executible
    invocation: QuestionMsg
    current_state: Optional[ResponseChain]
    final_state: Optional[ResponseMsg]


class Function(Executible):
    @classmethod
    def __factory__(cls):
        return Function.parse

    @classmethod
    def parse(cls, fn):
        ref = GlobalRef(fn)
        in_mold, out_mold, dst = extract_molds_from_function(fn)
        inst = cls(ref=ref, in_mold=in_mold, out_mold=out_mold)
        inst.__doc__ = dst.doc()
        return inst

    def __call__(self, *args, **kwargs):
        return self.ref.get_instance()(*args, **kwargs)

    def run(self, ctx: ExecContext):
        input = self.in_mold.mold_dict(ctx.invocation.data, Conversion.TO_OBJECT)
        output = self.ref.get_instance()(**input)
        ctx.final_state = ResponseMsg(
            previous=ctx.invocation.cake(),
            data=self.out_mold.mold_dict(output, Conversion.TO_JSON),
        )
