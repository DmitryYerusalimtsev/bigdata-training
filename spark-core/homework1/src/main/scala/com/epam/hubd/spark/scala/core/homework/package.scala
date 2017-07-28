package com.epam.hubd.spark.scala.core

/**
  * Created by Dmytro_Yerusalimtsev on 7/28/2017.
  */
package object homework {
  def isEmpty(x: String) = Option(x).forall(_.isEmpty)
}
