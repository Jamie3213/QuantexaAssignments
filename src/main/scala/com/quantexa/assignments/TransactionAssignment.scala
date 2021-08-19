package com.quantexa.assignments

import transactions.TransactionCaseClasses._
import transactions.TransactionUtils._
import scala.annotation.tailrec
import scala.io.Source
import scala.util.Try

/* -------------------------------------------------------------------------- */
/*                           Transactions Assignment                          */
/* -------------------------------------------------------------------------- */

// Run with 'gradle -PmainClass=com.quantexa.assignments.TransactionAssignment run'

object TransactionAssignment {
    def main(args: Array[String]): Unit = {
        /* ------------------------- Read project resources ------------------------- */

        // The lines of the CSV file (dropping the first to remove the header)
        val transactionLines: Iterator[String] = Source.fromResource("transactions.csv").getLines.drop(1)

        //Here we split each line up by commas and construct Transactions
        val transactions: List[Transaction] = transactionLines.map { line =>
            val split = line.split(',')
            Transaction(split(0), split(1), split(2).toInt, split(3), split(4).toDouble)
        }.toList

        /* -------------------- Transform data and produce output ------------------- */

        // // Question 1
        // val question1ResultValue = getTransactionTotalsByDay(transactions)
        // question1ResultValue.foreach(println)

        // // Question 2
        // val question2ResultValue = getAverageTransactionsByAccountAndType(transactions)
        // question2ResultValue.foreach(println)

        // Question 3
        def calculateStatisticsForDay(day: Int, window: Int, transactions: List[Transaction]) = {
            // Make sure the window can be calculated for the day specified, e.g. a 5 day window can only be calculated
            // if the value of 'day' is at least 6. If the window doesn't make sense, throw an error.
            if (day - window <= 0) throw new IllegalArgumentException("Value of 'day' must be greater than 'window'")

            // Filter to transactions related to the specified window
            val transWindow: List[Transaction] = transactions.filter(transaction => 
                transaction.transactionDay >= day - window && transaction.transactionDay < day
            )

            // Group by account ID
            val transByAcc: Map[String, List[Transaction]] = transWindow.groupBy(_.accountId)

            // Calculate totals for each of the account categories of interst
            val accCategories: List[String] = List("AA", "CC", "FF")
 
            val aaTotal: Iterable[Map[String, List[Transaction]]] = transByAcc.collect { 
                // Filter transactions on category
                case(accountId: String, trans: List[Transaction]) if trans.filter(_.category == "AA").nonEmpty => 
                    Map(accountId -> trans.filter(_.category == "AA"))
            }

            aaTotal.toList
        }

        calculateStatisticsForDay(6, 5, transactions).map { case(m: Map[String, List[Transaction]]) => m.values}
    }
}