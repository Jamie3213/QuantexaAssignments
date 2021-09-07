package com.quantexa.assignments

import addresses.AddressCaseClasses._
import addresses.AddressUtils._
import scala.io.Source

/* -------------------------------------------------------------------------- */
/*                            Addresses Assignment                            */
/* -------------------------------------------------------------------------- */

// Run with 'gradle -PmainClass=com.quantexa.assignments.AddressAssignment run'

object AddressAssignment {
  def main(args: Array[String]): Unit = {
    /* ------------------------- Read project resources ------------------------- */

    // The lines of the CSV file (dropping the first to remove the header)
    val addressLines: Iterator[String] = Source.fromResource("address_data.csv").getLines.drop(1)

    // Convert each address line to an 'AddressData' class and return a list
    val occupancyData: List[AddressData] = addressLines.map { line =>
      val split = line.split(',')
      AddressData(split(0), split(1), split(2).toInt, split(3).toInt)
    }
    .toList
    .sortBy(_.fromDate)

    /* -------------------- Transform data and produce output ------------------- */

    def anyOverlap(customer: AddressData, customerData: List[AddressData]): Boolean = {
      val overlappingCustomers: List[AddressData] = customerData.collect { 
        case otherCustomer if overlap(customer, otherCustomer) => otherCustomer 
      }.toList
      if (overlappingCustomers.nonEmpty) true else false
    }

    def getNextCustomerGroup(customerData: List[AddressData], accumulator: List[AddressData]): List[AddressData] = customerData match {
      case tail if anyOverlap(tail.head, accumulator) => getNextCustomerGroup(tail.tail, accumulator :+ tail.head)
      case _ => accumulator
    }

    def getAllCustomerGroups(customerData: List[AddressData], accumulator: List[AddressData]): List[AddressData] = {
      val filteredCustomerData: List[AddressData] = customerData diff accumulator
      filteredCustomerData match {
        case list if list.nonEmpty => getAllCustomerGroups(list, accumulator ::: getNextCustomerGroup(list.tail, List(list.head)))
        case _ => accumulator
      }
    }

    // Initalise starting values
    val customerData: List[AddressData] = occupancyData.filter(_.addressId == "ADR000")
    getAllCustomerGroups(customerData.tail, List[AddressData]()).foreach(println)
    // println(getNextCustomerGroup(customerData.tail, List(customerData.head)))
  }
}
