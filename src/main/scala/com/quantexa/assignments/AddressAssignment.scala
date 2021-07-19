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

    /* -------------------- Transform data and produce output ------------------- */

    // Create a list whose elements are lists of overlapping 'AddressData' instances
    val overlappingCustomers: List[List[AddressData]] = findOverlappingAddresses(occupancyData)

    // Create AddressGroupedData classes from each of the lists of overlapping customers
    val finalList: List[AddressGroupedData] = overlappingCustomers.map { list =>
      createAddressGroupedData(list)
    }

    // Print results
    println(s"Number of groups: ${finalList.size}")
    finalList.foreach(println)
  }
}
