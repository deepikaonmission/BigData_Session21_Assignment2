//<<<<<<<<<<<<<<------------------- TASK 2 ------------------->>>>>>>>>>>>>>>>>
//Task 2 is solved using map-side join and sql query to show the time comparison between the two techniques


import org.apache.spark.sql.{Column, Row, SQLContext, SparkSession}  //Explanation is already given in Assignment18.1

object Session21Assign2 extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Session21Assign2")
    .config("spark.sql.warehouse.dir", "file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 21/Assignments/Assignment2")
    .getOrCreate()
  //Explanation is already given in Assignment 18.1

  //setting path of winutils.exe
  System.setProperty("hadoop.home.dir", "F:/Softwares/winutils")
  //winutils.exe needs to be present inside HADOOP_HOME directory, else below error is returned:
  //error: java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.

  // Fact table
  val flights = spark.sparkContext.parallelize(List(
    ("SEA", "JFK", "DL", "418", "7:00"),
    ("SFO", "LAX", "AA", "1250", "7:05"),
    ("SFO", "JFK", "VX", "12", "7:05"),
    ("JFK", "LAX", "DL", "424", "7:10"),
    ("LAX", "SEA", "DL", "5737", "7:10")))

  // Dimension table
  val airports = spark.sparkContext.parallelize(List(
    ("JFK", "John F. Kennedy International Airport", "New York", "NY"),
    ("LAX", "Los Angeles International Airport", "Los Angeles", "CA"),
    ("SEA", "Seattle-Tacoma International Airport", "Seattle", "WA"),
    ("SFO", "San Francisco International Airport", "San Francisco", "CA")))

  // Dimension table
  val airlines = spark.sparkContext.parallelize(List(
    ("AA", "American Airlines"),
    ("DL", "Delta Airlines"),
    ("VX", "Virgin America")))


  //<<<<<<<<<<<<<<<<<<<<------------ MAP-SIDE JOIN --------------->>>>>>>>>>>>>>>>>>>
  //Collecting airports and airlines data as map i.e. (key-value pairs)
  val airportsMap = spark.sparkContext.broadcast(airports.map{case(a,b,c,d) => (a,c)}.collectAsMap())
  val airlinesMap = spark.sparkContext.broadcast(airlines.collectAsMap())

  airportsMap.value.foreach(x => println(x))
  //************REFER Screenshot 1 for output************
  airlinesMap.value.foreach(x => println(x))
  //************REFER Screenshot 2 for output************

  //Performing Map side join
  val result = flights.map{
                         case(a,b,c,d,e) =>
                            (airportsMap.value.get(a).get,
                             airportsMap.value.get(b).get,
                             airlinesMap.value.get(c).get,d,e)}
  result.foreach(x => println(x))
  //************REFER Screenshot 3 for output************


  //<<<<<<<<<<<<<<<<<-------------- SQL QUERY ------------------->>>>>>>>>>>>>>>>>>
  import spark.implicits._       //to convert rdd to dataframe this import is required
  //Converting RDDs to Dataframes
  flights.toDF("src","dest","airline_code","distance","time").createOrReplaceTempView("flights")
  airports.toDF("airport_code","airport_name","location_name","location_code").createOrReplaceTempView("airports")
  airlines.toDF("airline_code","airline_name").createOrReplaceTempView("airlines")

  spark.sql("select * from flights").show()
  //************REFER Screenshot 4 for output************
  spark.sql("select * from airports").show()
  //************REFER Screenshot 5 for output************
  spark.sql("select * from airlines").show()
  //************REFER Screenshot 6 for output************

  //created a table flightsAirports by joining fights and airports tables, so get source and destination name
  spark.sql("select t1.src,t2.location_name as srcName,t1.dest,t3.location_name as destName,t1.airline_code,t1.distance,t1.time from flights t1" +
    " left outer join airports t2" +
    " on t1.src = t2.airport_code" +
    " left outer join airports t3" +
    " on t1.dest = t3.airport_code").createOrReplaceTempView("flightsAirports")
  spark.sql("select * from flightsAirports").show()
  //************REFER Screenshot 7 for output************

  //below query gives required result
  spark.sql("select t1.srcName as source,t1.destName as destination,t2.airline_name as airline,t1.distance,t1.time from flightsAirports t1" +
    " left outer join airlines t2" +
    " on t1.airline_code = t2.airline_code").show()
  //************REFER Screenshot 8 for output************
}
