package com.quantexa.assignments

import addresses.AddressCaseClasses._
import addresses.Helpers._
import scala.io.Source

object AddressAssignment {
  def main(args: Array[String]): Unit = {
    // The lines of the CSV file (dropping the first to remove the header)
    val addressLines: Iterator[String] = Source.fromResource("test_addresses.csv").getLines.drop(1)

    // Convert each address line to an 'AddressData' class and return a list
    val occupancyData: List[AddressData] = addressLines.map { line =>
      val split = line.split(',')
      AddressData(split(0), split(1), split(2).toInt, split(3).toInt)
    }.toList

    // Create a list whose elements are lists of overlapping 'AddressData' instances
    val overlappingCustomers: List[List[AddressData]] = findOverlappingAddresses(occupancyData)

    // Create AddressGroupedData classes from each of the lists of overlapping customers
    val finalList: List[AddressGroupedData] = overlappingCustomers.map { list =>
      createAddressGroupedData(list)
    }

    // Print results: each case class and the total number of groups
    finalList.foreach(println)
    println(finalList.length)

  }
}
