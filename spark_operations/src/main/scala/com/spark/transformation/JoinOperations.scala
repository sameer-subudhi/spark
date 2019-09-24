package com.spark.transformation

import com.spark.schemas.{Department, Employee, EmployeeDetails, EmployeeFullDetail, PatitalInfo, States}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object JoinOperations {


  def datasetJoinA1(spark: SparkSession, employees: Dataset[Employee], department: Dataset[Department])
  : Dataset[EmployeeDetails] = {

    import spark.implicits._

    val columnNames = Seq("deptid")
    val joinConditions = columnNames.map(name => $"x.$name" === $"y.$name").reduce(_ && _)

    val employeeDetails = employees.as("x").joinWith(department.as("y"),
      $"x.deptid" === $"y.deptid", "left")
      .map(x =>
        EmployeeDetails(
          x._1.empid,
          x._1.empname,
          x._1.salary,
          if (Option(x._2).isDefined) x._2.deptid else x._1.deptid,
          if (Option(x._2).isDefined) x._2.deptname else null)
      )
    employeeDetails
  }


  def datasetJoinA2(spark: SparkSession, employees: Dataset[Employee], department: Dataset[Department])
  : Dataset[EmployeeDetails] = {

    import spark.implicits._

    val columnNames = Seq("deptid")
    val joinConditions = columnNames.map(name => $"x.$name" === $"y.$name").reduce(_ && _)

    val employeeDetails = employees.as("x").joinWith(department.as("y"),
      joinConditions, "left")
      .map(x => {
        val hasRight = Option(x._2).isDefined
        EmployeeDetails(
          x._1.empid,
          x._1.empname,
          x._1.salary,
          if (hasRight) x._2.deptid else x._1.deptid,
          if (hasRight) x._2.deptname else null)
      })
    employeeDetails
  }


  def datasetJoinA3(spark: SparkSession, employees: Dataset[Employee], department: Dataset[Department])
  : Dataset[EmployeeDetails] = {

    import spark.implicits._

    val columnNames = Seq("deptid")
    val joinConditions = columnNames.map(name => $"x.$name" === $"y.$name").reduce(_ && _)

    val employeeDetails = employees.as("x").joinWith(department.as("y"),
      joinConditions, "left")
      .map {
        case (x, null) =>
          EmployeeDetails(
            x.empid,
            x.empname,
            x.salary,
            Some(3),
            "Unknown")
        case (x, y) =>
          EmployeeDetails(
            x.empid,
            x.empname,
            x.salary,
            y.deptid,
            y.deptname)
      }
    employeeDetails
  }


  def datasetJoinA4(spark: SparkSession, employees: Dataset[Employee],
                    department: Dataset[Department], states: Dataset[States])
  : Dataset[EmployeeFullDetail] = {

    import spark.implicits._

    val columnNames = Seq("deptid")
    val joinConditions = columnNames.map(name => $"x.$name" === $"y.$name").reduce(_ && _)

    val employeeDetails = employees.as("x").joinWith(department.as("y"),
      $"x.deptid" === $"y.deptid", "inner")
      .joinWith(states.as("z"), $"_1.stateid" === states("stateid"), "inner")
      .map { case ((x, y), z) => EmployeeFullDetail(
        x.empid,
        x.empname,
        x.salary,
        y.deptid,
        y.deptname,
        z.statename)
      }
    employeeDetails
  }


  def dataframeJoinA1(spark: SparkSession, employees: Dataset[Employee], department: Dataset[Department])
  : Dataset[EmployeeDetails] = {

    import spark.implicits._

    val columnNames = Seq("deptid")
    val joinConditions = columnNames.map(name => $"x.$name" === $"y.$name").reduce(_ && _)

    val employeeDetails = employees.as("x").join(department.as("y"),
      joinConditions, "left")
      .select($"x.empid",
        $"x.empname",
        $"x.salary",
        $"y.deptid",
        $"y.deptname").as[EmployeeDetails]
    employeeDetails
  }


  def dataframeJoinA2(spark: SparkSession, employees: Dataset[Employee], department: Dataset[Department])
  : Dataset[PatitalInfo] = {

    import spark.implicits._

    val columnNames = Seq("deptid")
    val joinConditions = columnNames.map(name => $"x.$name" === $"y.$name").reduce(_ && _)

    // getAs is used pull a column value and assign to case class
    val employeeDetails = employees.map(x => (x.empid, x.empname)).toDF().as("x")
      .crossJoin(department.map(x => (x.deptid, x.deptname)).distinct().toDF().as("y"))
      .map(x => PatitalInfo(
        x.getAs[Int]("x._1"),
        x.getAs[String]("x._2"),
        x.getAs[Int]("y._1"),
        x.getAs[String]("y._2")
      ))
    employeeDetails
  }


  def dataframeJoinA3(spark: SparkSession, employees: Dataset[Employee], department: Dataset[Department])
  : DataFrame = {

    employees.createOrReplaceTempView("employees")
    department.createOrReplaceTempView("department")

    val employeeDetails = spark.sql("select * from employees e join department d on e.deptid == d.dpetid")

    employeeDetails
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Operations")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val employees = spark.read.option("delimiter", ",").schema(Encoders.product[Employee].schema)
      .csv("../employee.csv").as[Employee]
    val department = spark.read.option("delimiter", ",").schema(Encoders.product[Department].schema)
      .csv("../department.csv").as[Department]
    val states = spark.read.option("delimiter", ",").schema(Encoders.product[States].schema)
      .csv("../states.csv").as[States]

    // joining 2 dataset using joinWith
    val way1 = datasetJoinA1(spark, employees, department)
    val way2 = datasetJoinA2(spark, employees, department)
    val way3 = datasetJoinA3(spark, employees, department)
    // joining 3 dataset using joinWith
    val way4 = datasetJoinA4(spark, employees, department, states)

    way1.show()
    way2.show()
    way3.show()
    way4.show()

    // join returns a dataframe
    val way5 = dataframeJoinA1(spark, employees, department)
    way5.show()

    // cross join
    val way6 = dataframeJoinA2(spark, employees, department)
    way6.show()

    //joining 2 dataframe using sql like syntax
    val way7 = dataframeJoinA3(spark, employees, department)
    way7.show()

    spark.stop()
  }
}
