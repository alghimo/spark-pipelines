package org.alghimo.sparkPipelines

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

/**
  * Created by alghimo on 10/30/2016.
  */
trait WithSharedSparkSession extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
    @transient var spark: SparkSession = null

    override def beforeAll(): Unit = {
        super.beforeAll()

        spark = SparkSession.builder()
            .master("local[*]")
            .getOrCreate()
    }

    override def afterAll(): Unit = {
        super.afterAll()
        spark.stop()
    }
}
