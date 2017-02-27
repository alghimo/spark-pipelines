# Spark Pipelines
The main purpose of this package is to make it easier to get consistent pipelines that you can re-run and keep up to date.
You can also implement input and output validation, track the combined inputs / outputs of the pipeline, and easily switch your datasources.
For instance, you could have a pipeline running on Csv files in your local environment, and using Hive tables for your Prod environment.

## Components
### Data Manager
The data manager is the way you can abstract your inputs / outputs from the storage. An example implementation is the HiveDataManager.
The main idea behind is that you define your resources in a config file, associating a key to a certain resource. From there, the data manager reconstruct a DataFrame.

### Stage
A stage is a runnable step. All what's required to implement is the "run" method, but you should also define your inputs / outputs if you want to keep track of them, and optionally, you can override the validateInput / validateOutput methods.

### Pipeline
A pipeline is just a set of Stages, and is considered a Stage itself. This means that in your pipeline, one or more of your stages can also be pipelines, allowing for easy composition.

## Examples
### Using the HiveDataManager
By default, the HiveDataManager will try to load the configuration file "hive.conf", that must be available in your classpath.
A sample hive.conf file could look like this:

```
tables {
  table1    = {
    db = "test",
    table = "table1"
  },
  projected_table1 = {
    db = "test",
    table = "table1",
    select = ["col1 as newCol1", "col3"]
  },
  filtered_table1 = {
    db = "test",
    table = "table1",
    filter = "col1 = 3"
  },
  filtered_and_projected_table1 = {
    db = "test",
    table = "table1",
    select = ["col2", "col3"]
    filter = "col1 < 3"
  }
```

Here, you can see the most basic definition of a table (table1), that will simply take the whole table from Hive and return it.
This also shows the other two things you can do:
- select: This property takes a list of string expressions. You can use it to select just a subset of the columns, rename them, or do anything you could normally do using the DataFrame's "selectExpr" method.
- filter: Allows you to filter the table using a string sql expression, the same way you would use in a DataFrame's filter method.

### Creating your own Stages
Stages are quite straight forward to create, here's an example:

```scala
class Table3Stage(override val dataManager: DataManager, @transient override val spark: SparkSession)
    extends Stage(name = "Table3Stage", description = Some("This stage will generate a table"))(dataManager, spark) {
    // Here you define the input keys used by this stage
    override def inputs = Set("regular_table", "filtered_table")
    // Output keys used in the stage
    override def outputs = Set("table3")

    // The logic of your stage.
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
```

### Creating a Pipeline
Creating Pipelines is also very easy:

```scala
val table3Stage = new Table3Stage(dataManager, spark)
val table4Stage = new Table4Stage(dataManager, spark)
val pipeline = new Pipeline(
    name = "Test Pipeline",
    stages = Array(table3Stage, table4Stage),
    description = Some("Test two-stage pipeline")
)(
    dataManager = dataManager,
    spark = spark
)
```

If you want to nest a pipeline, you can do it like this:
```scala
val myStage1 = new MyStage1(dataManager, spark)
val myStage2 = new MyStage2(dataManager, spark)
val innerPipeline = new Pipeline(
    name = "Inner Pipeline",
    stages = Array(myStage1, myStage2),
    description = Some("Inner pipeline")
)(
    dataManager = dataManager,
    spark = spark
)
val myStage3 = new MyStage3(dataManager, spark)
val pipeline = new Pipeline(
    name = "Nested Pipeline",
    stages = Array(innerPipeline, myStage3),
    description = Some("Nested pipeline")
)(
    dataManager = dataManager,
    spark = spark
)
```

You can find more examples browsing the tests.

## Building the package
You can build the project using SBT. The current build is for Scala 2.11 and Spark 2.0.1.