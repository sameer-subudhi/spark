package com.spark.schemas

case class EmployeeDetails(empid: Int,
                           empname: String,
                           salary: Option[Double],
                           deptid: Option[Int],
                           deptname: String)
