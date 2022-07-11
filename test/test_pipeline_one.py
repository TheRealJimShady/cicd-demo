# Databricks notebook source
# MAGIC %pip install -U nutter chispa

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from src.pipeline_one import utils

# COMMAND ----------

# https://github.com/microsoft/nutter
from runtime.nutterfixture import NutterFixture, tag
# https://github.com/MrPowers/chispa
from chispa.dataframe_comparer import *

class TestFixturePipelineOne(NutterFixture):
  def __init__(self):
    self.code2_table_name = "summed_groups"
    self.code1_num_entries = 10
    NutterFixture.__init__(self)
    
  def run_code1_arbitrary_files(self):
    utils.sum_groups(spark, self.code2_table_name)
    
  def assertion_code1_arbitrary_files(self):
    df = spark.read.table(self.code2_table_name)
    assert(df.count() == self.code1_num_entries)

# COMMAND ----------

result = TestFixturePipelineOne().execute_tests()
print(result.to_string())
is_job = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().isDefined()
if is_job:
  result.exit(dbutils)
