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

    // Create a Map showing all overlapping customer groups for each address
    val overlappingCustomerGroupsByAddress: Map[String, List[List[AddressData]]] = occupancyData.groupBy(_.addressId)
      .map { case (addressId: String, customerData: List[AddressData]) =>
        val accumulator: List[List[AddressData]] = List(getNextCustomerGroup(customerData.tail, List(customerData.head)))
        val customerGroups: List[List[AddressData]] = getAllCustomerGroups(customerData, accumulator)
        Map(addressId -> customerGroups)
      }.reduce(_ ++ _)

    // For each list over customer groups, create an instance of AddressGroupedData classes
    val finalResult: List[AddressGroupedData] = overlappingCustomerGroupsByAddress.keys
      .map { addressId => 
        overlappingCustomerGroupsByAddress(addressId).map { list => createAddressGroupedData(list) }
      }
      .toList
      .flatten
      .sortBy(group => (group.addressId, group.startDate))
    
    // Print the result
    println(s"Number of groups: ${finalResult.length}")
    finalResult.foreach(println)
  }
}
