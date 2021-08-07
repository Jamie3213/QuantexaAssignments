package com.quantexa.assignments.transactions

object TransactionCaseClasses {
    case class Transaction(
        transactionId: String,
        accountId: String,
        transactionDay: Int,
        category: String,
        transactionAmount: Double
    )

    case class Question1Result(
        transactionDay: Int,
        transactionTotal: Double
    )

    case class Question2Result(
        accountId: String,
        categoryAvgValueMap: Map[String, Double]
    )
}