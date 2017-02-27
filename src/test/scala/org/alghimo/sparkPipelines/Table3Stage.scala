package org.alghimo.sparkPipelines

import org.apache.spark.sql.SparkSession

/**
  * Created by alghimo on 10/30/2016.
  */
class Table3Stage(override val dataManager: DataManager, @transient override val spark: SparkSession)
    extends Stage(name = "Table3Stage", description = Some("This stage will generate a table"))(dataManager, spark) {
    override def inputs = Set("regular_table", "filtered_table")
    override def outputs = Set("table3")

    override def run() = {
        val table1 = dataManager.get("regular_table")
        val table2 = dataManager.get("filtered_table")

        import spark.implicits._
        val table3 = table1.as('t1)
            .join(table2.as('t2), $"t1.col1"===$"t2.col1", "inner")
            .select($"t1.col1", $"t2.col3")

        dataManager.save("table3", table3)
    }
}
