 from pyspark import SparkContext, SparkConf
 from pyspark.sql import SparkSession, HiveContext
 SparkContext.setSystemProperty("hive.metastore.uris", "thrift://nn1:9083")
 sparkSession = (SparkSession
                 .builder
                 .appName('hive_connection')
                 .enableHiveSupport()
                 .getOrCreate())
 df_load = sparkSession.sql('USE MEDIX')
 df_load = sparkSession.sql('SELECT COUNT(*) FROM EXT_TABLE_VISITAS')
 df_load.show()