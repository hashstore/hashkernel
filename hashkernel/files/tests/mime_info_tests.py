from hashkernel.files.mime_info import mime_infos


def test_dict():
    html_ = mime_infos["HTML"]
    assert html_.mime == "text/html"
    assert html_.ext == ["htm", "html"]
