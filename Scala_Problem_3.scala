package HDFSFileFormat

import org.apache.spark.sql.{DataFrame, SparkSession}

object Scala_Problem_3 {
  def main(args : Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master(master = "local")
      .appName(name = "This is scala problem-3 program")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:////home/sterlite/Spark/spark-events")
      .config("spark.history.fs.logDirectory", "file:////home/sterlite/Spark/spark-events")
      .getOrCreate()

    val df = sparkSession.read.format(source = "csv").option("header", "true")
     .load(path = "hdfs://hadoop.localhost:9000/test/employee_address_details.csv")
    //df.show()

    val df1 = sparkSession.read.format(source = "csv").option("header", "true")
      .load(path = "hdfs://hadoop.localhost:9000/test/Employee_Business_Details.csv")
    //df1.show()

    val df2 = sparkSession.read.format(source = "csv").option("header", "true")
      .load(path = "hdfs://hadoop.localhost:9000/test/Employee_personal_details.csv")
    //df2.show()

    //creating tempviews
    df.createOrReplaceTempView(viewName= "ea")
    df1.createOrReplaceTempView(viewName= "eb")
    df2.createOrReplaceTempView(viewName= "ep")


   var sq1 = sparkSession.sql(sqlText = "SELECT AVG(salary), MAX(salary), MIN(salary) FROM EP WHERE AgeinYrs between 30.00 and 40.00")
   sq1.show()

    var sq2 = sparkSession.sql(sqlText= "SELECT MIN(salary) from eb JOIN ep on eb.Emp_ID where AgeinYrs between 30.00 AND 40.00")
    sq2.show()

    var sq3 = sparkSession.sql(sqlText= "SELECT MAX(salary) from emp_business JOIN emp_personal on emp_business.Emp_ID where AgeinYrs between 30.00 AND 40.00")
    sq3.show()

    var a = sparkSession.sql(sqlText = "select Year_of_Joining, COUNT(*) AS No_of_Emp FROM eb GROUP BY Year_of_Joining")
    a.show()

    var b = sparkSession.sql(sqlText = "SELECT Emp_Id,Salary,LastHike,CAST('14% AS INT) AS Sal FROM business AS details")
    b.show()

    var sq4 = sparkSession.sql(sqlText = "select Count(Email),Year_of_Joining FROM emp_business Group By(Year_of_Joining)")
    sq4.show()

    var sq5 = sparkSession.sql(sqlText = "select Count(Email),Year_of_Joining FROM emp_business Group By(Year_of_Joining) ORDER by (Year_of_joining)")
    sq5.show()

   

  }

}
