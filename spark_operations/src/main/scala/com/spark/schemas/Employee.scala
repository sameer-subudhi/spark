package com.spark.schemas

case class Employee(empid: Int,
                    empname: String,
                    salary: Option[Double],
                    deptid: Option[Int]
                   )
