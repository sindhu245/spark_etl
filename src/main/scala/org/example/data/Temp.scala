package org.example.data

import org.apache.spark.sql.functions.sum
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime

object Temp extends App{
//for(i <- 1 to 10) {
//  var current_time = System.currentTimeMillis()
//  //  println(new )
//
//  var temp: Long = 0
//  if (i >= 5) {
//    temp = current_time
//  }
//  else {
//    println()
//    temp = current_time + (2 * 365 * 24 * 60 * 60 * 1000)
//  }
//
//  var hello = current_time
//
//
//  def epochToDate(epochMillis: Long): String = {
//    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
//    df.format(epochMillis)
//  }
//
//  //  println("-----"+ epochToDate(hello)+ "------------"+epochToDate(temp)+"-----"+epochToDate(1753092440626L))
//  ////  +"\n")
//  //  println(63072000000L+1690020440626L)
//}

  val cDateTime: LocalDateTime = LocalDateTime.now
  println(cDateTime)
  val nextyear = LocalDateTime.now.plusYears(2)
  println(LocalDateTime.parse(nextyear.toString).isAfter(cDateTime))


  val ctimestamp = Timestamp.valueOf(cDateTime)
  val nextTime = Timestamp.valueOf(nextyear)
//  println(nextTime-ctimestamp)
println(LocalDateTime.now)
  println(Timestamp.valueOf(nextyear))




////  println(Long.valueOf(nextyear))
//  val obj = new loadData()
//    val data3 = obj.readDataframe("dataset_3")
//  println("\n"+data3.agg(sum("is_expired")).first.get(0))


println("2025-07-22T17:43:16.328" > "2023-07-22T17:43:16.328")

  val list: List[Int] = List(1,2,3,4,5)
  println(list.find(_>3))
}

