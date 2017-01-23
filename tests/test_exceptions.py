import pytest
from mentos import exceptions as exc



def test_connect_error_string():
    e = exc.ConnectError("endpoint")
    assert e.endpoint == "endpoint"



def test_master_redirect_location_passing():
    e = exc.MasterRedirect("location")
    assert e.location == "location"

