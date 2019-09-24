package com.spark.schemas

case class EmployeeFullDetail(empid: Int,
                              empname: String,
                              salary: Option[Double],
                              deptid: Option[Int],
                              deptname: String,
                              statename: String)
