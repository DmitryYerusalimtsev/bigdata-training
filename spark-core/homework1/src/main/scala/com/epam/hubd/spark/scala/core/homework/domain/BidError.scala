package com.epam.hubd.spark.scala.core.homework.domain

case class BidError(date: String, errorMessage: String) {

  override def toString: String = s"$date,$errorMessage"
}
