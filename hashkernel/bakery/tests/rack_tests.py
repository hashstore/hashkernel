import os
import tempfile
from io import BytesIO

from hashkernel import to_json, utf8_reader
from hashkernel.ake import Cake
from hashkernel.bakery.rack import HashRack


def test_Bundle():
    b1 = HashRack()
    assert b1.content() == "[[], []]"
    empty_rack_cake = b1.cake()
    with tempfile.NamedTemporaryFile("w", delete=False) as w:
        w.write(b1.content())
    b2 = HashRack().parse(b1.content())
    u_f = Cake.from_file(w.name)
    os.unlink(w.name)
    u2 = b2.cake()
    assert u_f == u2
    assert empty_rack_cake == u2
    assert empty_rack_cake == u2
    b1["a"] = empty_rack_cake
    udk_bundle_str = f'[["a"], ["{empty_rack_cake}"]]'
    assert str(b1) == udk_bundle_str
    u1 = b1.cake()
    assert u1 != u2
    b2.parse(utf8_reader(BytesIO(bytes(b1))))
    assert str(b2) == udk_bundle_str
    assert b2.size() == 56
    u2 = b2.cake()
    assert u1 == u2
    del b2["a"]
    u2 = b2.cake()
    assert empty_rack_cake == u2
    assert b1["a"] == empty_rack_cake
    assert b1.get_cakes() == [empty_rack_cake]
    assert [k for k in b1] == ["a"]
    assert [k for k in b2] == []
    assert b1.get_name_by_cake(empty_rack_cake) == "a"
    assert HashRack(to_json(b1)) == b1
    assert HashRack.ensure_it(to_json(b1)) == b1
    assert len(b1) == 1
    assert str(b1) == udk_bundle_str
    assert hash(b1) == hash(udk_bundle_str)
