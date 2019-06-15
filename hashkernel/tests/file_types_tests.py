from hs_build_tools.pytest import eq_


def test_dict():
    from hashkernel.file_types import file_types
    html_ = file_types["HTML"]
    eq_(html_.mime,'text/html')
    eq_(html_.ext,['htm', 'html'])
