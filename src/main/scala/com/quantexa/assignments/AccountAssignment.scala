package com.quantexa.assignments

import accounts.AccountCaseClasses._
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.SparkSession
import scala.io.Source

/* -------------------------------------------------------------------------- */
/*                             Account Assingment                             */
/* -------------------------------------------------------------------------- */

// Run with 'gradle -PmainClass=com.quantexa.assignments.AccountAssignment run'

object AccountAssignment {
  def main(args: Array[String]): Unit = {
    /* ------------------------ Start local Spark cluster ----------------------- */

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("AccountAssignment")
      .getOrCreate()

    // Set logger level to WARN
    Logger.getRootLogger.setLevel(Level.WARN)

    /* ------------------- Get project resources and read CSVs ------------------ */

    // Get file paths from files stored in project resources
    val customerCSV = getClass.getResource("/customer_data.csv").getPath
    val accountCSV = getClass.getResource("/account_data.csv").getPath
    val testDataCSV = getClass.getResource("/test_customer_account_data.csv").getPath

    // Importing spark implicits to allow functions such as dataframe.as[T]
    import spark.implicits._

    // Customer source data
    val customerDF = spark.read
      .option("header","true")
      .csv(customerCSV)

    // Account source data
    val accountDF = spark.read
      .option("header","true")
      .csv(accountCSV)

    // Create Datasets from source Dataframes
    val customerDS = customerDF.as[CustomerData]
    val accountDS = accountDF.withColumn("balance", $"balance".cast("long"))
      .as[AccountData]

    /* -------------------- Transform data and produce output ------------------- */

    // Join the two source Datasets into a single Dataset based on the customerId column of each of the
    // Datasets and use a new case class to the define the schema of each row.
    //
    // Since there may be customers in one source Dataset but not in another, we need handle three
    // scenarios when pattern matching:
    //
    //  - Customer is in customerDS and not in accountDS
    //  - Customer is in accountDS and not in customerDS
    //  - Customer is in both customerDS and accountDS
    //
    val customerAccountDataDS = customerDS
      .joinWith(accountDS, customerDS.col("customerId") === accountDS.col("customerId"), "full_outer")
      .map { 
        case(c, null) => CustomerAccountData(c.customerId, c.forename, c.surname, "", 0L)
        case(null, a) => CustomerAccountData(a.customerId, "", "", a.accountId, a.balance)
        case(c, a) => CustomerAccountData(c.customerId, c.forename, c.surname, a.accountId, a.balance)
      }

    // Collect all rows to the driver node and convert to a list of case classes
    val customerAccountData: List[CustomerAccountData] = customerAccountDataDS.collect
      .toList

    // After collecting rows to the driver node, create a Map showing the accounts associated 
    // with each customer
    val accountMap: Map[String, Seq[AccountData]] = accountDS.collect
      .toList
      .groupBy(_.customerId)
      .map { case(customerId: String, accountData: List[AccountData]) => Map(customerId -> accountData.toSeq) }
      .reduce(_ ++ _)

    // Group by customer Id and calculate aggregates
    val customerAccountOuput: List[CustomerAccountOutput] = customerAccountData.groupBy(_.customerId). map { case (customerId: String, accountData: List[CustomerAccountData]) =>
      CustomerAccountOutput(
          customerId,
          accountData.head.forename,
          accountData.head.surname,
          accountMap.getOrElse(customerId, Seq[AccountData]()),
          accountMap.getOrElse(customerId, Seq[AccountData]()).length,
          accountData.map { _.balance }.sum,
          accountData.map { _.balance }.sum / accountData.length.toDouble
      )
    }
    .toList
    .sortBy(_.customerId)

    // Convert to Dataset and show top 20 rows
    val customerAccountOuputDS = customerAccountOuput.toDS()
    customerAccountOuputDS.show
  }
}    
