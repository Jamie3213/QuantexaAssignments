package com.quantexa.assignments.addresses

object AddressCaseClasses {

  case class AddressData(
                          customerId: String,
                          addressId: String,
                          fromDate: Int,
                          toDate: Int
                        )


  case class AddressGroupedData(
                                 group: Long,
                                 addressId: String,
                                 customerIds: Seq[String],
                                 startDate: Int,
                                 endDate: Int
                               )
}
