def test_dict():
    from hashkernel.files.types import file_types

    html_ = file_types["HTML"]
    assert html_.mime == "text/html"
    assert html_.ext == ["htm", "html"]