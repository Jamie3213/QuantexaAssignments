package com.quantexa.assignments.accounts

import AccountCaseClasses._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, TypedColumn}
import scala.collection.mutable.ListBuffer 

/* -------------------------------------------------------------------------- */
/*                          Create custom aggregators                         */
/* -------------------------------------------------------------------------- */
/*
 * For each of the aggregate columns we want to appear in our final, aggregated Dataset,
 * we define a type-safe Aggregator which tells Spark how to compute aggregate values on
 * data across each worker node in the cluster.
 *
 * For a given Aggregator we need to define a set of key functions:
 *
 *   - zero: this is the initial value
 *   - reduce: this tells a worker what to do with each value
 *   - merge: this tells any two workers how to combine their reduced results
 *   - finish: this defines how we compute the final value from the merged results across the cluser
 *   - bufferEncoder: this defines the intermediary encoding
 *   - outputEncoder: this defines the encoding of the final, aggregated result
 * 
 * Once a custom Aggregator is defined, it can be called within the .agg method like any standard
 * Spark aggregator (e.g. count, sum, mean).
 *
 */

object AccountUtils {
    // Get the distinct forename of the customer with the ID 'customerId'
    val distinctForenameAggregator: TypedColumn[CustomerAccountData, String] =
        new Aggregator[CustomerAccountData, Set[String], String] {
            override def zero: Set[String] = Set[String]()
            override def reduce(emptyList: Set[String], accountData: CustomerAccountData): Set[String] = emptyList + accountData.forename
            override def merge(workerA: Set[String], workerB: Set[String]): Set[String] = workerA.union(workerB)
            override def finish(reduction: Set[String]): String = reduction.head
            override def bufferEncoder: Encoder[Set[String]] = ExpressionEncoder[Set[String]]
            override def outputEncoder: Encoder[String] = ExpressionEncoder[String]
        }.toColumn

    // Get the distinct surname of the customer with the ID 'customerId'
    val distinctSurnameAggregator: TypedColumn[CustomerAccountData, String] =
        new Aggregator[CustomerAccountData, Set[String], String] {
            override def zero: Set[String] = Set[String]()
            override def reduce(emptyList: Set[String], accountData: CustomerAccountData): Set[String] = emptyList + accountData.surname
            override def merge(workerA: Set[String], workerB: Set[String]): Set[String] = workerA.union(workerB)
            override def finish(reduction: Set[String]): String = reduction.head
            override def bufferEncoder: Encoder[Set[String]] = ExpressionEncoder[Set[String]]
            override def outputEncoder: Encoder[String] = ExpressionEncoder[String]
        }.toColumn
    
    // Aggregate all of a customer's accounts into a Seq of AccountData
    val accountsAggregator: TypedColumn[CustomerAccountData, Seq[AccountData]] =
        new Aggregator[CustomerAccountData, ListBuffer[AccountData], Seq[AccountData]] {
            override def zero: ListBuffer[AccountData] = ListBuffer[AccountData]()
            override def reduce(emptyList: ListBuffer[AccountData], data: CustomerAccountData): ListBuffer[AccountData] = {
                val account = AccountData(data.customerId, data.accountId, data.balance)
                emptyList += account
            }
            override def merge(workerA: ListBuffer[AccountData], workerB: ListBuffer[AccountData]): ListBuffer[AccountData] = workerA ++ workerB

            // Need to filter out missing account IDs since these will otherwise produce an incorrect count in customers who were
            // only present in the customerDS
            override def finish(reduction: ListBuffer[AccountData]): Seq[AccountData] = reduction.filter(_.accountId != "").toSeq

            override def bufferEncoder: Encoder[ListBuffer[AccountData]] = ExpressionEncoder[ListBuffer[AccountData]]
            override def outputEncoder: Encoder[Seq[AccountData]] = ExpressionEncoder[Seq[AccountData]] 
        }.toColumn

    // Count the number of accounts associated with a customerId
    val numberOfAccountsAggregator: TypedColumn[CustomerAccountData, Int] =
        new Aggregator[CustomerAccountData, ListBuffer[String], Int] {
            override def zero: ListBuffer[String] = ListBuffer[String]()
            override def reduce(emptyList: ListBuffer[String], accountData: CustomerAccountData): ListBuffer[String] = emptyList += accountData.accountId
            override def merge(workerA: ListBuffer[String], workerB: ListBuffer[String]): ListBuffer[String] = workerA ++ workerB

            // Need to filter out blank string accounts since these were customers who only appeared in the
            // customerDS, otherwise the count of accounts would be incorrect (i.e. non-zero)
            override def finish(reduction: ListBuffer[String]): Int = reduction.filter(_ != "").size

            override def bufferEncoder: Encoder[ListBuffer[String]] = implicitly(ExpressionEncoder[ListBuffer[String]])
            override def outputEncoder: Encoder[Int] = implicitly(Encoders.scalaInt)
        }.toColumn

    // Calculate the total balance across all accounts associated with a customerId
    val totalBalanceAggregator: TypedColumn[CustomerAccountData, Long] = 
        new Aggregator[CustomerAccountData, Long, Long] {
            override def zero: Long = 0L
            override def reduce(zeroLong: Long, accountData: CustomerAccountData): Long = zeroLong + accountData.balance
            override def merge(workerA: Long, workerB: Long): Long = workerA + workerB
            override def finish(reduction: Long): Long = reduction
            override def bufferEncoder: Encoder[Long] = implicitly(Encoders.scalaLong)
            override def outputEncoder: Encoder[Long] = implicitly(Encoders.scalaLong)
        }.toColumn

    // Calculate the average balance across all accounts associated with a customerId
    val averageBalanceAggregator: TypedColumn[CustomerAccountData, Double] = 
        new Aggregator[CustomerAccountData, ListBuffer[Long], Double] {
            override def zero: ListBuffer[Long] = ListBuffer[Long]()
            override def reduce(emptyList: ListBuffer[Long], accountData: CustomerAccountData): ListBuffer[Long] = emptyList += accountData.balance
            override def merge(workerA: ListBuffer[Long], workerB: ListBuffer[Long]): ListBuffer[Long] = workerA ++ workerB
            override def finish(reduction: ListBuffer[Long]): Double = reduction.sum / reduction.size.toDouble
            override def bufferEncoder: Encoder[ListBuffer[Long]] = implicitly(ExpressionEncoder[ListBuffer[Long]])
            override def outputEncoder: Encoder[Double] = implicitly(Encoders.scalaDouble)
        }.toColumn
}