package com.quantexa.assignments.transactions

import TransactionCaseClasses._

/* -------------------------------------------------------------------------- */
/*                  Transaction Assignment Utility Functions                  */
/* -------------------------------------------------------------------------- */

object TransactionUtils {
    /**
    * Returns a sequence of Question1Result case classes showing the total value of transactions
    * for each day, ordered by day
    *
    * @param transactions a list of Transactions case classes
    */
    def getTransactionTotalsByDay(transactions: List[Transaction]): Seq[Question1Result] = {
        transactions.groupBy(transaction => transaction.transactionDay)
            .map { case(key: Int, value: List[Transaction]) =>
                Question1Result(
                    value.head.transactionDay,
                    value.map(_.transactionAmount).sum
                )                
            }
            .toSeq
            .sortBy(_.transactionDay)
    }
}