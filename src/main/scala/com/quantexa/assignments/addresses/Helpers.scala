package com.quantexa.assignments.addresses

import AddressCaseClasses.{AddressData, AddressGroupedData}

import scala.util.Random

object Helpers {
  /**
   * Returns true if the two customers' date ranges overlap, else false
   *
   * @param x an instance of AddressData
   * @param y an instance of AddressData
   */
  def overlap(x: AddressData, y: AddressData): Boolean = {
    if ((x.toDate >= y.fromDate) && (x.fromDate <= y.toDate)) true else false
  }

  /**
   * Returns a list whose elements are lists of AddressData instances where the
   * the customers time at the address overlapped
   *
   * @param customerAddressData List of AddressData case classes objects (customers)
   * @return List of lists of overlapping AddressData classes
   */
  def findOverlappingAddresses(customerAddressData: List[AddressData]): List[List[AddressData]] = {
    customerAddressData.groupBy(_.addressId)
      .map { group =>
        group._2
          .combinations(2)
          .collect { case List(x, y) if overlap(x, y) => (x, y) }
          .flatMap { tuple => Seq(tuple._1, tuple._2) }
          .toList
          .distinct
      }
      .toList
      .filter(_.nonEmpty)
  }

  /**
   * Returns an AddressGroupedData instance from the provided list of AddressData instances
   *
   * @param overlappingCustomers List of overlapping AddressData objects (customers)
   * @return AddressGroupedData case class
   */
  def createAddressGroupedData(overlappingCustomers: List[AddressData]): AddressGroupedData = {
    // Prep case class attributes
    val group: Long = Random.nextInt(Int.MaxValue).toLong
    val addressId: String = overlappingCustomers.head.addressId
    val customerIds: Seq[String] = overlappingCustomers.map { _.customerId }
    val startDate: Int = overlappingCustomers.map { _.fromDate }.min
    val endDate: Int = overlappingCustomers.map { _.toDate }.max

    // Create new case class instance
    AddressGroupedData(group, addressId, customerIds, startDate, endDate)
  }
}
