from bigquery_utils import utils


def test_response_creation():
    res = utils.Response("test", 0, {"a": "b"})
    assert res.name == "test"
    assert res.code == 0
    assert res.additional == {"a": "b"}


def test_standardise_bigquery_ref():
    # ref that does not need to be changed
    simple_ref = "abc.def.ghi"
    # ref that does need to be changed
    colon_ref = "abc:def.ghi"

    assert utils.standardise_bigquery_ref(simple_ref) == simple_ref
    assert utils.standardise_bigquery_ref(colon_ref) == simple_ref


def test_validate_standardised_bigquery_reference():
    refs = [
        {"ref": "abc.def.ghi", "valid": True},
        {"ref": "abc.def", "valid": True},
        {"ref": "abc.def.ö", "valid": True},
        {"ref": "abc.def.!", "valid": False},
        {"ref": "a-b-c.d_ef_.g_h_i", "valid": True},
        {"ref": "ab_c.def.ghi", "valid": False},
        {"ref": "abc.de-f.ghi", "valid": False},
    ]

    for item in refs:
        assert utils.validate_standardised_bigquery_reference(item.get("ref"))[
            0
        ] == item.get("valid")
