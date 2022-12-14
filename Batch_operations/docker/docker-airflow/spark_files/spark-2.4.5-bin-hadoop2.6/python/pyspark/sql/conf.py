#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys

from pyspark import _NoValue, since
from pyspark.rdd import ignore_unicode_prefix

if sys.version_info[0] >= 3:
    basestring = str


class RuntimeConfig(object):
    """User-facing configuration API, accessible through `SparkSession.conf`.

    Options set here are automatically propagated to the Hadoop configuration during I/O.
    """

    def __init__(self, jconf):
        """Create a new RuntimeConfig that wraps the underlying JVM object."""
        self._jconf = jconf

    @ignore_unicode_prefix
    @since(2.0)
    def set(self, key, value):
        """Sets the given Spark runtime configuration property."""
        self._jconf.set(key, value)

    @ignore_unicode_prefix
    @since(2.0)
    def get(self, key, default=_NoValue):
        """Returns the value of Spark runtime configuration property for the given key,
        assuming it is set.
        """
        self._checkType(key, "key")
        if default is _NoValue:
            return self._jconf.get(key)
        else:
            if default is not None:
                self._checkType(default, "default")
            return self._jconf.get(key, default)

    @ignore_unicode_prefix
    @since(2.0)
    def unset(self, key):
        """Resets the configuration property for the given key."""
        self._jconf.unset(key)

    def _checkType(self, obj, identifier):
        """Assert that an object is of type str."""
        if not isinstance(obj, basestring):
            raise TypeError(
                "expected %s '%s' to be a string (was '%s')"
                % (identifier, obj, type(obj).__name__)
            )

    @ignore_unicode_prefix
    @since(2.4)
    def isModifiable(self, key):
        """Indicates whether the configuration property with the given key
        is modifiable in the current session.
        """
        return self._jconf.isModifiable(key)


def _test():
    import doctest
    import os

    import pyspark.sql.conf
    from pyspark.sql.session import SparkSession

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.conf.__dict__.copy()
    spark = (
        SparkSession.builder.master("local[4]").appName("sql.conf tests").getOrCreate()
    )
    globs["sc"] = spark.sparkContext
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(pyspark.sql.conf, globs=globs)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
