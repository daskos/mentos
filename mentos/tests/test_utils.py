
import pytest
import collections
from mentos import utils,exceptions as exc
import six
import socket
from subprocess import Popen, PIPE
from tornado import gen
import time


@pytest.fixture
def data():
    return ["foo", 1, "bar", 9]


def test_drain_on_list(data):

    result = list(utils.drain(data))

    assert len(data) == 0
    assert result == [9, "bar", 1, "foo"]


def test_drain_on_deque(data):
    data = collections.deque(data)

    result = list(utils.drain(data))

    assert len(data) == 0
    assert result == ["foo", 1, "bar", 9]


def test_drain_on_setdata(data):
    data = set(data)

    result = list(utils.drain(data))

    assert len(data) == 0
    assert set(result) == set(["foo", 1, "bar", 9])


def test_drain_on_dict():
    data = {"foo": 1, "bar": 9}

    result = {key: value for key, value in utils.drain(data)}

    assert len(data) == 0
    assert result == {"foo": 1, "bar": 9}


def test_parse_duration():
    for k, v in six.iteritems(utils.POSTFIX):
        assert utils.parse_duration("3 " + k) == v * 3.0

    with pytest.raises(Exception):
        utils.parse_duration("3megawats")


def test_encode_data():
    assert utils.encode_data(b"stuff") == 'c3R1ZmY='


def test_decode_data():
    assert utils.decode_data('c3R1ZmY=') == b"stuff"




@pytest.mark.gen_test
def test_master_info_endpoint():
    master = utils.MasterInfo("localhost:9091")

    assert master.detector == None



    url = yield master.get_endpoint()
    assert url == 'http://localhost:9091'

    assert master.info == {'address': {'hostname': 'localhost', 'port': 9091}}

    master.redirected_uri("localhost:9092")

    url = yield master.get_endpoint()

    assert url == 'http://localhost:9092'

    assert master.info == {'address': {'hostname': 'localhost', 'port': 9092}}



@pytest.mark.gen_test
def test_master_info_endpoint_no_port():
    master = utils.MasterInfo("localhost")

    assert master.detector == None
    url = yield master.get_endpoint()
    assert url == 'http://localhost:5050'
    assert master.info == {'address': {'hostname': 'localhost', 'port': 5050}}



@pytest.mark.gen_test(run_sync=False,timeout=60)
def test_master_info_zk():
    master = utils.MasterInfo("zk://localhost:2181")

    assert master.detector != None


    url = yield master.get_endpoint()
    assert url != None
    assert master.info !=None

    assert master.info["hostname"] == "localhost"

    assert master.info["ip"] != None

    assert master.info["port"] in (5050,6060,7070)

    with pytest.raises(exc.NoRedirectException):
        master.redirected_uri("localhost:9092")

    url_again = yield master.get_endpoint()

    assert url_again != None

    assert url == url_again

    assert master.info != None

    assert master.info["hostname"] == "localhost"

    assert master.info["ip"] != None

    assert master.info["port"] in (5050, 6060,7070)


    if master.info["port"] == 5050:# pragma: no cover
        active = "mesos_master_0"
    elif master.info["port"] == 6060:# pragma: no cover
        active = "mesos_master_1"
    else:
        active = "mesos_master_2"


    old_info = master.info.copy()
    p = Popen(["docker-compose restart %s"%active], shell=True,
          stdout=PIPE, stderr=PIPE)

    a = p.wait()

    time.sleep(20)
    yield gen.sleep(5)

    url_again = yield master.get_endpoint()

    assert url_again != None

    assert url != url_again

    assert master.info != None

    assert master.info["hostname"] == "localhost"

    assert master.info["ip"] != None

    assert master.info["port"] in set([5050, 6060,7070]) - set([old_info["port"]])



