import pytest
from mock import patch
from extract.Extract import Events
from extract.MapHelper import MapEvents


def test_map_events_none_rdd():
    events = Events(None,{})
    with pytest.raises(Exception):
        events.map_events()


def test_map_events_none_dictionary(spark_context):
    rdd = spark_context.parallelize([1,2,3])
    events = Events(rdd,None)
    with pytest.raises(Exception):
        events.map_events()


def test_map_events_rdd_notinstance_RDD():
    rdd = {}
    events = Events(rdd,{})
    with pytest.raises(Exception):
        events.map_events()


def test_map_events_dictionary_notinstance_dict(spark_context):
    rdd = spark_context.parallelize([1,2,3])
    events = Events(rdd,rdd)
    with pytest.raises(Exception):
        events.map_events()


def test_map_events_success(spark_context):
    rdd = spark_context.parallelize([1,2,3])
    rdd1 = spark_context.parallelize([('a',4),('b',5),('c',6)])
    rdd2 = spark_context.parallelize([('a',4),('b',5),('c',6),('a',7),('b',8),('c',9)])
    events = Events(rdd,{})
    with patch.object(MapEvents, 'get_event_fields') as mock_event_fields:
        with patch.object(MapEvents, 'map_events') as mock_map_events:
            mock_event_fields.return_value = rdd1
            mock_map_events.return_value = rdd2
            result = events.map_events().collect()
            print(result)
            assert len(result) == 3
            for r in result:
                if r[0] == 'a':
                    assert r[1].data == [4,7]
                elif r[0] == 'b':
                    assert r[1].data == [5,8]
                elif r[0] == 'c':
                    assert r[1].data == [6,9]


def test_map_events_none_eventfields(spark_context):
    rdd = spark_context.parallelize([1,2,3])
    rdd1 = spark_context.parallelize([('a',4),('b',5),('c',6)])
    rdd2 = spark_context.parallelize([('a',4),('b',5),('c',6),('a',7),('b',8),('c',9)])
    events = Events(rdd,{})
    with patch.object(MapEvents, 'get_event_fields') as mock_event_fields:
        with pytest.raises(Exception):
            mock_event_fields.return_value = None
            events.map_events()


def test_map_events_eventfields_not_rdd(spark_context):
    rdd = spark_context.parallelize([1,2,3])
    rdd1 = spark_context.parallelize([('a',4),('b',5),('c',6)])
    rdd2 = spark_context.parallelize([('a',4),('b',5),('c',6),('a',7),('b',8),('c',9)])
    events = Events(rdd,{})
    with patch.object(MapEvents, 'get_event_fields') as mock_event_fields:
        with pytest.raises(Exception):
            mock_event_fields.return_value = {}
            events.map_events()


def test_map_events_none_mappedevents(spark_context):
    rdd = spark_context.parallelize([1,2,3])
    rdd1 = spark_context.parallelize([('a',4),('b',5),('c',6)])
    events = Events(rdd,{})
    with patch.object(MapEvents, 'get_event_fields') as mock_event_fields:
        with patch.object(MapEvents, 'map_events') as mock_map_events:
            with pytest.raises(Exception):
                mock_event_fields.return_value = rdd1
                mock_map_events.return_value = None
                events.map_events()


def test_map_events_mappedevents_not_rdd(spark_context):
    rdd = spark_context.parallelize([1,2,3])
    rdd1 = spark_context.parallelize([('a',4),('b',5),('c',6)])
    events = Events(rdd,{})
    with patch.object(MapEvents, 'get_event_fields') as mock_event_fields:
        with patch.object(MapEvents, 'map_events') as mock_map_events:
            with pytest.raises(Exception):
                mock_event_fields.return_value = rdd1
                mock_map_events.return_value = {}
                events.map_events()