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
            .map { case(day: Int, trans: List[Transaction]) =>
                Question1Result(
                    trans.head.transactionDay,
                    trans.map(_.transactionAmount).sum
                )                
            }
            .toSeq
            .sortBy(_.transactionDay)
    }

    /**
    * Returns a sequence of Question2Result case classes showing the account ID and the average value of the
    * transactions of the specified type, ordered by account ID
    *
    * @param transactions a list of Transaction case classes
    */
    def getAverageTransactionsByAccountAndType(transactions: List[Transaction]): Seq[Question2Result] = {
        transactions.groupBy(transaction => (transaction.accountId, transaction.category))
            .map { case((accountId: String, category: String), trans: List[Transaction]) =>
                Question2Result(
                    accountId,
                    Map(category -> (trans.map(_.transactionAmount).sum / trans.length))
                )
            }
            .toSeq
            .sortBy(_.accountId)
    }
}