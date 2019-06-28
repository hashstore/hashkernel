

def test_dict():
    from hashkernel.file_types import file_types

    html_ = file_types["HTML"]
    assert html_.mime == "text/html"
    assert html_.ext == ["htm", "html"]
