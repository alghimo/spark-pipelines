package org.alghimo.sparkPipelines

import org.alghimo.sparkPipelines.dataManager.DataManager
import org.apache.spark.sql.SparkSession

/**
  * Created by alghimo on 10/30/2016.
  */
class Table4Stage(override val dataManager: DataManager, @transient override val spark: SparkSession)
    extends Stage(name = "Table4Stage", description = Some("Generates table4 from the output of another stage"))(dataManager, spark) {
    override def inputs = Set("table3")
    override def outputs = Set("table4")

    override def run() = {
        val table3 = dataManager.get("table3")
        val newRows = table3.selectExpr("col1 + 1 AS col1", "col3 + 1 AS col3")
        val table4 = table3.union(newRows)
        table4.show
        dataManager.save("table4", table4)
    }
}
