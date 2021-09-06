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
    }.toList

    // /* -------------------- Transform data and produce output ------------------- */

    // For each customer, create sets showing other customers that customer overlaps with
    val overlappingCustomersByAddress: Map[String, List[Set[AddressData]]] = occupancyData.groupBy(_.addressId)
      .map { case (addressId: String, customers: List[AddressData]) =>
        val overlappingCustomers: List[Set[AddressData]] = customers.map { customer => 
          val filteredCustomers: List[AddressData] = customers.filter(_ != customer)
          filteredCustomers.collect { case otherCustomer if overlap(customer, otherCustomer) =>
            List(customer, otherCustomer)
          }
          .flatten
          .toSet
        }
        .distinct
        .filter(_.nonEmpty)

        Map(addressId -> overlappingCustomers)
      }.reduce(_ ++ _)

    // Aggregate the sets based on whether or not they intersect any of the other sets for that address
    val addressIds: List[String] = occupancyData.map { _.addressId }.toList
      .distinct

    val customerGroupsByAddress: Map[String, List[Set[AddressData]]] = addressIds.map { addressId => 
      val customerListForAddress: List[Set[AddressData]] = overlappingCustomersByAddress(addressId)
      val customerGroups: List[Set[AddressData]] = customerListForAddress.map { set => 
        getIntersectingSets(set, customerListForAddress).toSet
      }.toList
      Map(addressId -> customerGroups.distinct)
    }.reduce(_ ++ _)

    customerGroupsByAddress("ADR000").foreach(println)

    // // Create a list whose elements are lists of overlapping 'AddressData' instances
    // val overlappingCustomers: List[List[AddressData]] = findOverlappingAddresses(occupancyData)

    // // Create AddressGroupedData classes from each of the lists of overlapping customers
    // val finalList: List[AddressGroupedData] = overlappingCustomers.map { list =>
    //   createAddressGroupedData(list)
    // }

    // // Print results
    // println(s"Number of groups: ${finalList.size}")
    // finalList.foreach(println)
  }
}
