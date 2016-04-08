#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - @todo

This implements the code to store and save data about tweets

IMPORTANT NOTE: All times are in UTC.  They must be either naive and represent
UTC or contain valid tzinfo.
"""

__author__ = 'Patrick Butler'
__email__ = 'pbutler@killertux.org'

import pycassa
import datetime
from ..utils import now, dt_to_ts, uuid_to_dt

from pycassa.util import convert_time_to_uuid


class InvalidDefinitionException(Exception):
    pass


class ColumnFamily(object):
    """Record a set of numerical stats"""
    name = None
    columns = []
    super = False

    def __init__(self, pool):
        """@todo: to be defined

        :param pool: the connection pool with keypace to use
        """
        if self.__class__.name is None:
            raise InvalidDefinitionException("Name undefined in class: " +
                                             self.__class__.__name__)

        self._pool = pool
        self._cf = pycassa.ColumnFamily(self._pool, self.__class__.name)

    @classmethod
    def _get_class_keys(cls):
        """@todo: Docstring for __get_class_keys

        :param arg1: @todo
        :returns: @todo

        """
        arg_keys = {i: i for i in [
            "comparator_type", "subcomparator_type", "merge_shards_chance",
            "column_validation_classes", "key_cache_size", "row_cache_size",
            "gc_grace_seconds", "read_repair_chance", "comment"
            "default_validation_class", "key_validation_class",
            "min_compaction_threshold", "max_compaction_threshold",
            "key_cache_save_period_in_seconds", "replicate_on_write",
            "row_cache_save_period_in_seconds", "compaction_strategy_options",
            "row_cache_provider", "key_alias", "compaction_strategy",
            "row_cache_keys_to_save", "compression_options",
        ]}
        arg_keys.update({'default_validation_class': 'default_type',
                         'key_validation_class': 'key_type',
                         'comparator_type': 'column_name_type',
                         'subcomparator_type': 'subcolumn_name_type',

                        })
        kwargs = {}

        for pc_arg, cls_arg in arg_keys.iteritems():
            if hasattr(cls, cls_arg):
                kwargs[pc_arg] = getattr(cls, cls_arg)

        kwargs['column_validation_classes'] = cls.columns \
                if cls.columns else None
        kwargs['super'] = cls.super
        return kwargs

    @classmethod
    def create(cls, sys, keyspace):
        """@todo: Docstring for create_cf

        :param sys: @todo
        :param keysapce: @todo
        :returns: @todo

        """
        kwargs = cls._get_class_keys()
        if cls.name not in sys.get_keyspace_column_families(keyspace).keys():
            sys.create_column_family(keyspace, cls.name, **kwargs)

    @classmethod
    def alter(cls, sys, keyspace):
        """@todo: Docstring for create_cf

        :param sys: @todo
        :param keysapce: @todo
        :returns: @todo

        """
        kwargs = cls._get_class_keys()
        for k in ["super", "comparator_type", "subcomparator_type",
                  "key_validation_class"]:
            if k in kwargs:
                del kwargs[k]
        sys.alter_column_family(keyspace, cls.name, **kwargs)

    @classmethod
    def create_or_alter(cls, sys, keyspace):
        """@todo: Docstring for create_cf

        :param sys: @todo
        :param keysapce: @todo
        :returns: @todo

        """
        if cls.name not in sys.get_keyspace_column_families(keyspace).keys():
            cls.create(sys, keyspace)
        else:
            cls.alter(sys, keyspace)

    def batch(self):
        return self._cf.batch()

    def insert(self, *args, **kwargs):
        batch = kwargs.get('batch')

        if batch is None:
            self._cf.insert(*args, **kwargs)
        else:
            del kwargs['batch']
            batch.insert(*args, **kwargs)

    def remove(self, *args, **kwargs):
        batch = kwargs.get('batch')

        if batch is None:
            self._cf.remove(*args, **kwargs)
        else:
            del kwargs['batch']
            batch.remove(*args, **kwargs)

    def get(self, *args, **kwargs):
        self._cf.get(*args, **kwargs)

    def xget(self, *args, **kwargs):
        self._cf.xget(*args, **kwargs)


class WideTimeColumnFamily(ColumnFamily):
    """A generic  class for storingnumerical stats
    start
    interval
    """
    column_name_type = pycassa.types.TimeUUIDType()

    def __init__(self, pool):
        """@todo: to be defined

        :param pool: the connection pool with keypace to use
        """
        cls = self.__class__
        super(WideTimeColumnFamily, self).__init__(pool)
        if not hasattr(cls, 'start_ts'):
            self.__class__.start_ts = dt_to_ts(self.__class__.start)

    def row_key(self, _time):
        cls = self.__class__
        _time = int(dt_to_ts(_time) - cls.start_ts)
        _time //= cls.interval
        return str(int(_time))

    def col_key(self, _time):
        return convert_time_to_uuid(_time, randomize=True)

    def insert(self, time, data, batch=None):
        rkey = self.row_key(time)
        ckey = convert_time_to_uuid(time, randomize=True)

        if batch is None:
            self._cf.insert(rkey, {ckey: data})
        else:
            batch.insert(rkey, {ckey: data})
        return ckey

    def remove(self, uuid):
        t = uuid_to_dt(uuid)
        k = self.row_key(t)
        col_type = "columns"
        if self.__class__.super:
            col_type = "super_column"
        self._cf.remove(k, **{col_type: uuid})

    def xget(self, start=None, stop=None, bsize=1000):
        cls = self.__class__
        if start is None:
            start = cls.start
        if stop is None:
            stop = now()

        place = start
        while True:  # start <= stop:
            kstart = self.row_key(place)
            total = self._cf.get_count(kstart,
                                       column_start=start,
                                       column_finish=stop,)

            s = start
            seen = 0
            while seen < total:
                tmp = self._cf.get(kstart, column_start=s,
                                   column_finish=stop,
                                   column_count=bsize)
                itr = tmp.iteritems()
                if seen > 0:  # column start/finish are inclusive so skip
                    itr.next()

                for k, v in itr:
                    yield k, v
                    s = max(s, uuid_to_dt(k))
                    seen += 1

            start = s
            if place > stop:
                break
            place += datetime.timedelta(seconds=cls.interval)
        return

    def get(self, start=None, stop=None, bsize=1000):
        return list(self.xget(start, stop, bsize))


class CounterColumnFamily(WideTimeColumnFamily):
    super = False
    default_type = pycassa.COUNTER_COLUMN_TYPE
    column_name_type = pycassa.DATE_TYPE
    #column_name_type = pycassa.types.TimeUUIDType()
    sub_interval = "m"

    def row_key(self, name, _time):
        cls = self.__class__
        _time = int(dt_to_ts(_time) - cls.start_ts)
        _time //= cls.interval
        return name + "|" + str(int(_time))

    def col_key(self, name, _time):
        si = self.__class__.sub_interval
        if si == "m":
            _time = _time.replace(second=0, microsecond=0)
        elif si == "h":
            _time = _time.replace(minute=0, second=0, microsecond=0)
        elif si == "d":
            _time = _time.replace(hour=0, minute=0, second=0, microsecond=0)
        return _time

    def add(self, name, value=1, _time=None):
        if _time is None:
            _time = now()
        rkey = self.row_key(name, _time)
        ckey = self.col_key(name, _time)
        self._cf.add(rkey, ckey, value)

    def get_value(self, name, _time):
        rkey = self.row_key(name, _time)
        ckey = self.col_key(name, _time)
        try:
            return self._cf.get(rkey, [ckey]).values()[0]
        except pycassa.cassandra.c10.ttypes.NotFoundException:
            return 0


class StatColumnFamily(WideTimeColumnFamily):
    super = False
    default_type = pycassa.INT_TYPE
    column_name_type = pycassa.DATE_TYPE
    #column_name_type = pycassa.types.TimeUUIDType()
    sub_interval = "m"

    def row_key(self, name, _time):
        cls = self.__class__
        _time = int(dt_to_ts(_time) - cls.start_ts)
        _time //= cls.interval
        return name + "|" + str(int(_time))

    def col_key(self, name, _time):
        si = self.__class__.sub_interval
        if si == "m":
            _time = _time.replace(second=0, microsecond=0)
        elif si == "h":
            _time = _time.replace(minute=0, second=0, microsecond=0)
        elif si == "d":
            _time = _time.replace(hour=0, minute=0, second=0, microsecond=0)
        return _time

    def insert(self, name, value, _time=None, batch=None):
        if _time is None:
            _time = now()
        rkey = self.row_key(name, _time)
        ckey = self.col_key(name, _time)
        if batch is None:
            self._cf.insert(rkey, {ckey: value})
        else:
            batch.insert(rkey, {ckey: value})

    def get(self, *args, **kwargs):
        return list(self.xget(*args, **kwargs))

    def xget(self, name, start=None, stop=None, bsize=1000):
        cls = self.__class__
        if start is None:
            start = cls.start
        if stop is None:
            stop = now()

        place = start
        while True:  # start <= stop:
            kstart = self.row_key(name, place)
            total = self._cf.get_count(kstart,
                                       column_start=start,
                                       column_finish=stop,)
            s = start
            seen = 0
            while seen < total:
                tmp = self._cf.get(kstart, column_start=s,
                                   column_finish=stop,
                                   column_count=bsize)
                itr = tmp.iteritems()
                if seen > 0:  # column start/finish are inclusive so skip
                    itr.next()

                for k, v in itr:
                    yield k, v
                    s = max(s, k)  # uuid_to_dt(k))
                    seen += 1

            start = s
            if place > stop:
                break
            place += datetime.timedelta(seconds=cls.interval)
        return

    #def get_value(self, name, _time):
    #    rkey = self.row_key(name, _time)
    #    ckey = self.col_key(name, _time)
    #    try:
    #        return self._cf.get(rkey, [ckey]).values()[0]
    #    except pycassa.cassandra.c10.ttypes.NotFoundException:
    #        return 0
