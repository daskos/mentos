from mentos.exceptions import ConnectError, MasterRedirect


def test_connect_error_string():
    e = ConnectError('endpoint')
    assert e.endpoint == 'endpoint'


def test_master_redirect_location_passing():
    e = MasterRedirect('location')
    assert e.location == 'location'
