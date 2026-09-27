#!/usr/bin/env python3
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

"""Print the physical plan for one SQL file. Does not execute it."""

import sys

from pyspark.sql import SparkSession

path = next(a for a in sys.argv[1:] if a.endswith(".sql"))
sql = open(path, encoding="utf-8", errors="replace").read()
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sql("USE tpcds_sf10_parquet")
spark.conf.set("spark.sql.vegam.enabled", "true")
df = spark.sql(sql)
print("PLAN_START")
print(df._jdf.queryExecution().executedPlan().toString())
print("PLAN_END")
spark.stop()
