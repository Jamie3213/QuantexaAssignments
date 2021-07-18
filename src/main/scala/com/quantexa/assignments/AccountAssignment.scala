package com.quantexa.assignments

import accounts.AccountCaseClasses._
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, TypedColumn}
import scala.collection.mutable.ListBuffer 
import scala.io.Source

/***
 * A common problem we face at Quantexa is having lots of disjointed raw sources of data and having to aggregate and collect
 * relevant pieces of information into hierarchical case classes which we refer to as Documents. This exercise simplifies
 * the realities of this with just two sources of high quality data however reflects the types of transformations we must
 * perform.
 *
 * You have been given customerData and accountData. This has been read into a DataFrame for you and then converted into a
 * Dataset of the given case classes.
 *
 * If you run this App you will see the top 20 lines of the Datasets provided printed to console
 *
 * This allows you to use the Dataset API which includes .map/.groupByKey/.joinWith ect.
 * But a Dataset also includes all of the DataFrame functionality allowing you to use .join ("left-outer","inner" ect)
 *
 * https://spark.apache.org/docs/latest/sql-programming-guide.html
 *
 * The challenge here is to group, aggregate, join and map the customer and account data given into the hierarchical case class
 * customerAccountoutput. We would prefer you to write using the Scala functions instead of spark.sql() if you choose to perform
 * Dataframe transformations. We also prefer the use of the Datasets API.
 *
 * Example Answer Format:
 *
 * val customerAccountOutputDS: Dataset[customerAccountOutput] = ???
 * customerAccountOutputDS.show(1000,truncate = false)
 *
 * +----------+-----------+----------+---------------------------------------------------------------------+--------------+------------+-----------------+
 * |customerId|forename   |surname   |accounts                                                             |numberAccounts|totalBalance|averageBalance   |
 * +----------+-----------+----------+---------------------------------------------------------------------+--------------+------------+-----------------+
 * |IND0001   |Christopher|Black     |[]                                                                   |0             |0           |0.0              |
 * |IND0002   |Madeleine  |Kerr      |[[IND0002,ACC0155,323], [IND0002,ACC0262,60]]                        |2             |383         |191.5            |
 * |IND0003   |Sarah      |Skinner   |[[IND0003,ACC0235,631], [IND0003,ACC0486,400], [IND0003,ACC0540,53]] |3             |1084        |361.3333333333333|
 * ...
 */
object AccountAssignment {
  def main(args: Array[String]): Unit = {
    // Run Spark on the local machine as a single-node cluster
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("AccountAssignment")
      .getOrCreate()

    // Set logger level to WARN
    Logger.getRootLogger.setLevel(Level.WARN)

    // Get file paths from files stored in project resources
    val customerCSV = getClass.getResource("/customer_data.csv").getPath
    val accountCSV = getClass.getResource("/account_data.csv").getPath
    val testDataCSV = getClass.getResource("/test_customer_account_data.csv").getPath

    // Importing spark implicits to allow functions such as dataframe.as[T]
    import spark.implicits._

    // Create DataFrames of sources
    val customerDF = spark.read
      .option("header","true")
      .csv(customerCSV)

    val accountDF = spark.read
      .option("header","true")
      .csv(accountCSV)

    //Create Datasets of sources
    val customerDS = customerDF.as[CustomerData]
    val accountDS = accountDF.withColumn("balance", $"balance".cast("long"))
      .as[AccountData]

    // Create a Dataset by joining the customer Dataset with the account Dataset
    val customerAccountDataDS = customerDS
      .joinWith(accountDS, customerDS.col("customerId") === accountDS.col("customerId"), "full_outer")
      .map { 
        // Customer exists in customerDS but not accountDS
        case(c, null) => CustomerAccountData(c.customerId, c.forename, c.surname, "", 0L)
        // Customer exists in accountDS but not customerDS
        case(null, a) => CustomerAccountData(a.customerId, "", "", a.accountId, a.balance)
        // Customer exists in both datasets
        case(c, a) => CustomerAccountData(c.customerId, c.forename, c.surname, a.accountId, a.balance)
      }

    // Number of accounts aggregator
    val numberOfAccountsAggregator: TypedColumn[CustomerAccountData, Int] =
      new Aggregator[CustomerAccountData, ListBuffer[String], Int] {
        override def zero: ListBuffer[String] = ListBuffer[String]()
        override def reduce(emptyList: ListBuffer[String], accountData: CustomerAccountData): ListBuffer[String] = emptyList += accountData.accountId
        override def merge(workerA: ListBuffer[String], workerB: ListBuffer[String]): ListBuffer[String] = workerA ++ workerB
        override def finish(reduction: ListBuffer[String]): Int = reduction.size
        override def bufferEncoder: Encoder[ListBuffer[String]] = implicitly(ExpressionEncoder[ListBuffer[String]])
        override def outputEncoder: Encoder[Int] = implicitly(Encoders.scalaInt)
      }.toColumn

    customerAccountDataDS.groupByKey(row => row.customerId)
      .agg(numberOfAccountsAggregator.name("numberAccounts"))
      .show
  }
}    
