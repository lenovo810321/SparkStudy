package cn.aura

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by 张宝玉 on 2018/10/4.
  */
object DepartmentAvgSalaryAndAgeStat {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DepartmentAvgSalaryAndAgeStat").master("local")
      .config("spark.sql.warehouse.dir", "C:\\Users\\张宝玉\\Desktop\\spark-warehouse")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val employee: DataFrame = spark.read.json("C:\\Users\\张宝玉\\Desktop\\employee.json")
    val department = spark.read.json("C:\\Users\\张宝玉\\Desktop\\department.json")

    println(employee.map(employee => 1).reduce(_+_))

    employee.printSchema()
    employee.createOrReplaceTempView("employee")

    val filter: DataFrame = spark.sql("select * from employee limit 4")
    filter.write.json("C:\\Users\\张宝玉\\Desktop\\filter.json")
    spark.sql("select * from employee limit 4").explain()

//    employee
//      // 先对employee进行过滤，只统计20岁以上的员工
//      .filter("age > 20")
//      // 需要跟department数据进行join，然后才能根据部门名称和员工性别进行聚合
//      // 注意：untyped join，两个表的字段的连接条件，需要使用三个等号
//      .join(department, $"depId" === $"id")
//      // 根据部门名称和员工性别进行分组
//      .groupBy(department("name"), employee("gender"))
//      // 最后执行聚合函数
//      .agg(avg(employee("salary")), avg(employee("age")))
//      // 执行action操作，将结果显示出来
//      .show()


    employee.filter("age > 20")
      .join(department, $"depID" === $"id")
      .groupBy(department("name"), employee("gender"))
      .agg(avg(employee("salary")), avg(employee("age")))
      .show()
  }
}
