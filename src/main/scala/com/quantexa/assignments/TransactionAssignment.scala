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

        // Question 1
        val question1ResultValue: Seq[Question1Result] = getTransactionTotalsByDay(transactions)
        question1ResultValue.foreach(println)

        // // Question 2
        val question2ResultValue: Seq[Question2Result] = getAverageTransactionsByAccountAndType(transactions)
        question2ResultValue.foreach(println)

        // Question 3
        // Get list of days in transaction data
        val days: List[Int] = transactions.map { _.transactionDay }
            .distinct
        
        val question3ResultValue: Seq[Question3Result] = days.flatMap { day => 
            calculateStatisticsForDay(transactions, day, 5)
        }.sortBy(_.transactionDay)

        question3ResultValue.foreach(println)
    }
}