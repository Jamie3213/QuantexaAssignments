package com.quantexa.assignments.accounts

/* -------------------------------------------------------------------------- */
/*                Define case classes for use in Spark Datasets               */
/* -------------------------------------------------------------------------- */

object AccountCaseClasses {
  case class CustomerData(
                           customerId: String,
                           forename: String,
                           surname: String
                         )

  case class AccountData(
                          customerId: String,
                          accountId: String,
                          balance: Long
                        )

  case class CustomerAccountData(
                          customerId: String,
                          forename: String,
                          surname: String,
                          accountId: String,
                          balance: Long
                        )

  case class CustomerAccountOutput(
                                    customerId: String,
                                    forename: String,
                                    surname: String,
                                    //Accounts for this customer
                                    accounts: Seq[AccountData],
                                    //Statistics of the accounts
                                    numberAccounts: Int,
                                    totalBalance: Long,
                                    averageBalance: Double
                                  )
}
