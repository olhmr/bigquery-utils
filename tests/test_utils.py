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
