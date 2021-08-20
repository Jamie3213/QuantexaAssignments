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
        transactions.groupBy(_.transactionDay)
            .map { case(day: Int, trans: List[Transaction]) =>
                Question1Result(
                    trans.head.transactionDay,
                    trans.map { _.transactionAmount }.sum
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
        transactions.groupBy(_.accountId)
            .map { case(accId: String, trans: List[Transaction]) =>
                // Get a list of account categories for a given account ID
                val accCats: List[String] = trans.map { _.category }.toList.distinct
                
                // For each account, create a list of tuples containing the account category and the
                // average value for that category
                val accCatAvergaes: Map[String, Double] = accCats.map { cat =>
                    // Filter transactions on category value and calculate average
                    val filteredTrans: List[Transaction] = trans.filter(tran => tran.category == cat)
                    val catAverage: Double = filteredTrans.map { _.transactionAmount }.sum / filteredTrans.length
                    
                    // Return tuple
                    (cat, catAverage)
                }.toMap

                Question2Result(accId, accCatAvergaes)
            }
            .toSeq
            .sortBy(_.accountId)
    }

    /**
    * Returns a Map of the total value of all transactions related to the specified category for each account.
    * 
    * @param transactions a list of Transaction case classes
    * @param category the account transaction category on which to filter transactions
    */
    def calculateTotalsForCategoryByAccount(transactions: List[Transaction], category: String): Map[String, Double] = {
            // Group by the account ID
            transactions.groupBy(_.accountId)
                .collect { 
                    case(accId: String, trans: List[Transaction]) if trans.filter(_.category == category).nonEmpty =>
                        (accId, trans.filter(_.category == category))
                }
                .map { case(accId: String, trans: List[Transaction]) =>
                    // Calculate total for the category
                    val catTotal: Double = trans.map { _.transactionAmount }.sum
                    (accId, catTotal)
                }
        }

    /**
    * Returns a List of Question3Result case classes for the specified day and window size.
    *
    * @param transactions a list of Transaction case classes
    * @param day the day to count the window back from
    * @param window the size of the window in days
    */
    def calculateStatisticsForDay(transactions: List[Transaction], day: Int, window: Int): Seq[Question3Result] = {
        // Make sure the window can be calculated for the day specified, e.g. a 5 day window can only be calculated
        // if the value of 'day' is at least 6 - if the window doesn't make sense, throw an error.
        if (day - window <= 0) throw new IllegalArgumentException("Value of 'day' must be greater than 'window'")

        // Filter to transactions related to the specified window
        val windowTrans: List[Transaction] = transactions.filter(transaction => 
            transaction.transactionDay >= day - window && transaction.transactionDay < day
        )

        // Calculate totals for each of the account categories of interst
        val aaTotals: Map[String, Double] = calculateTotalsForCategoryByAccount(windowTrans, "AA")
        val ccTotals: Map[String, Double] = calculateTotalsForCategoryByAccount(windowTrans, "CC")
        val ffTotals: Map[String, Double] = calculateTotalsForCategoryByAccount(windowTrans, "FF")

        // Calculate the maximum transaction value for each account
        val maxTrans: Map[String, Double] = windowTrans.groupBy(_.accountId)
            .map { case(accId: String, trans: List[Transaction]) =>
                val max: Double = trans.map { _.transactionAmount }.max
                (accId, max)
            }

        // Calculate the average transaction value for each account
        val avgTrans: Map[String, Double] = windowTrans.groupBy(_.accountId)
            .map { case(accId: String, trans: List[Transaction]) =>
                val avg: Double = trans.map { _.transactionAmount }.sum / trans.length
                (accId, avg)
            }
        
        // Return a List of Question3Result case classes
        windowTrans.groupBy(_.accountId)
            .map { case(accId: String, trans: List[Transaction]) =>
                Question3Result(
                    day,
                    accId,
                    maxTrans.getOrElse(accId, 0),
                    avgTrans.getOrElse(accId, 0),
                    aaTotals.getOrElse(accId, 0),
                    ccTotals.getOrElse(accId, 0),
                    ffTotals.getOrElse(accId, 0)
                )
            }
            .toSeq
            .sortBy(_.accountId)
    }
}
