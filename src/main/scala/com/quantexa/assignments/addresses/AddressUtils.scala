package com.quantexa.assignments.addresses

import AddressCaseClasses._
import scala.annotation.tailrec
import scala.util.Random

/* -------------------------------------------------------------------------- */
/*                    Address Assignment Utility Functions                    */
/* -------------------------------------------------------------------------- */

object AddressUtils {
  /**
    * Returns true if the customers overlap, else false.
    *
    * @param x an instance of AddressData
    * @param y an instance of AddressData
    */
  def overlap(x: AddressData, y: AddressData): Boolean = {
    if ((x.toDate >= y.fromDate) && (x.fromDate <= y.toDate)) true else false
  }

  /**
    * Returns true if the customer overlaps with any customer in the list, else false.
    * 
    * @param  customer an instance of AddressData
    * @param  customerData a list of AddressData case classes          
    */
  def anyOverlap(customer: AddressData, customerData: List[AddressData]): Boolean = {
    val overlappingCustomers: List[AddressData] = customerData.collect { 
      case otherCustomer if overlap(customer, otherCustomer) => otherCustomer 
    }.toList
    if (overlappingCustomers.nonEmpty) true else false
  }

  /**
    * Returns the next group of customers in the list such that every customer 
    * in the group overlaps with at least one other customer. For a given list
    * of customers, 'customerData' should be the tail of the list of customers
    * for which the group is required, and 'accumulator' should be a list with
    * a single element whose value is the value of the head of 'customerData'.
    *
    * @param customerData a list of AddressData case classes
    * @param accumulator a list of AddressData case classes to which results will be appended
    */
  @tailrec
  def getNextCustomerGroup(customerData: List[AddressData], accumulator: List[AddressData]): List[AddressData] = customerData match {
    case tail if tail.isEmpty => accumulator
    case tail if anyOverlap(tail.head, accumulator) => getNextCustomerGroup(tail.tail, accumulator :+ tail.head)
    case _ => accumulator
  }

  /**
    * Returns a list whose elements are lists of customers who overlapped with at least one other
    * customer in the group. The value of 'accumulator' should be the first group of customers returned
    * from calling the getNextCustomerGroup function.
    *
    * @param customerData
    * @param accumulator
    */
  @tailrec
  def getAllCustomerGroups(customerData: List[AddressData], accumulator: List[List[AddressData]]): List[List[AddressData]] = {
    // Get the index corresponding to the customer that the next list should start at
    val nextGroupStartIndex: Int = customerData.indexOf(accumulator.last.last) + 1

    // Get the next customer slice, i.e. all customers less the ones that have already appeared
    // in a group
    val nextSlice: List[AddressData] = nextGroupStartIndex match {
      case nextGroupStartIndex if nextGroupStartIndex > customerData.length => List[AddressData]()
      case _ => customerData.splitAt(nextGroupStartIndex)._2
    }

    // Get the next group of overlapping customers
    val nextGroup: List[AddressData] = nextSlice match {
      case nextSlice if nextSlice.nonEmpty => getNextCustomerGroup(nextSlice.tail, List(nextSlice.head))
      case _ => List[AddressData]()
    }

    // Rinse and repeat
    nextGroup match {
      case nextGroup if nextGroup.isEmpty => accumulator
      case _ => getAllCustomerGroups(customerData, accumulator :+ nextGroup)
    }
  }

  /**
    * Returns an AddressGroupedData instance from the provided list of AddressData instances
    *
    * @param overlappingCustomers a list of overlapping AddressData instances
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
