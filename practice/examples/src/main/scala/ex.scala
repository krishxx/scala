import java.time.{LocalDateTime, Period, ZoneId}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.storage.StorageLevel
//package com.jasonfeist.spark.tika.example
import org.codehaus.janino

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object example {
  val allCols="maid,cid,gender,language,age_range,ts,ip,country,dt,doe,source,last_seen_file_name,birth_day,birth_month,birth_year,dou".split(",").toSeq;
  val schema = StructType(allCols.map(x=>StructField(x,StringType,true)))

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("examples").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    testAgeRangeConflict(sqlContext);

    //val (contData,validData)= makeContirbutorFile();

    //contData.show(5)

    //readFile()
    //println("Hello Scala World");
    //fun1

    /*sum(1,2,3)
    box("mltutor")*/
    //sumargs("Hello", "world")
    //testTruthDF(sqlContext)

    //testAgeRangeConflict(sqlContext)
  }

  def fun1: Unit = {
    println("function1")
  }

  def sum(args: Int*) = {
    var result = 0;
    for (arg <- args) result += arg;
    println(result)
    result
  }

  def box(s: String) { // Look carefully: no =
    val border = "-" * s.length + "--\n"
    println(border + "|" + s + "|\n" + border)
  }

  def words = scala.io.Source.fromFile("/usr/share/dict/words").mkString

  def t: Unit = {
    var x: Int = 1;
    var y: Int = 1;

    // x = if (x==1) y = 2;
    println(x + "," + y)
  }

  def readFile(): Unit = {
    import java.util.regex.Pattern
    //import sqlContext.implicits._


    //val spark = SparkGen.getSpark()
    val cols = "rawmaid,ts,lan,lat,mt1,mt2,ip,mt3,mt4,cc,app".split(",")
    //f4c34c32-2856-4dcb-863a-9ad17187c89e, 1542932713819,-4.2960277,-38.9844038,GPS,bground,45.4.28.114,16.0,0.0,BR,125
    val schema = StructType(cols.map(x => StructField(x, StringType, true)))

    val dashRemover = (maid: String) => {
      maid.replace("-", "")
    }
    val formatMAID = udf(dashRemover)

    //    val df = spark.read.option("delimiter", "|").schema(schema).csv("file:///home/srikrishna/projects/twinedata/twine_location_aaid_20181101_00_00000_9f6f482c.bz2").withColumn("rawmaid", formatMAID(col("rawmaid")))
    //    df.show()

    //val ipfilter = df.filter($"ip" === "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."+"([01]?\\d\\d?|2[0-4]\\d|25[0-5])$")
    //val international = sqlContext.sql("select count(distintct(rawmaid)) from location_data where count(rawmaid) > 1")
    //val filterdf = df.filter($"ip".isNotNull && $"ts".isNotNull && $ )

  }


  def sumargs(args: String*) = {
    var result = 0
    //for (arg <- args) result += arg
    print("result:" + result)
    result
  }


  def testTruthDF(sqlContext: SQLContext) {
    import org.apache.spark.sql;
    //val spark = SparkGen.getSpark()
    //val sc = spark.sparkContext;
    import sqlContext.implicits._

    val tVal = Seq(("0", "4"), ("4", "2"), ("8", "2"));
    val iVal = Seq(("0", "1", "2"), ("4", "2", "2"), ("8", "3", "2"));

    val tdf = tVal.toDF("maid", "dt")
    val idf = iVal.toDF("maid", "cid", "dt")

//    val dtTruthSet = tdf.toDF("maid", "verified_dt").select("maid", "verified_dt").distinct();
//    dtTruthSet.show()
//    //val inputDataset=IterativeBroadcastJoiner.iterativeJoin(10, inputDatasetIn, dtTruthSet, "maid", "left").persist(StorageLevel.MEMORY_AND_DISK);
//    val inputDataset = idf.join(dtTruthSet, "maid")
//
//
//    val maidsWithDtCount = inputDataset.filter("verified_dt is null").groupBy(col("maid")).agg(countDistinct("dt").cast(StringType).as("unique_dt_label_count")).filter("cast(unique_dt_label_count as int)>1").persist(StorageLevel.MEMORY_AND_DISK);
//    val countOfMaidsWithNoTrueDt = maidsWithDtCount.count();


//    val avgCnt = tdf.groupBy("maid").agg()
//    avgCnt.show()
    //      EventManager.pushEvent(IngestionEvent(eventDate=LocalDateTime.now(),key="Coun of maids with no True DT",value=countOfMaidsWithNoTrueDt+"",description="Count of maids with no truth device type attached"))
    //      logger.info("Count of maids with no truth deveice type label attahed : "+countOfMaidsWithNoTrueDt)
    //      //IterativeBroadcastJoiner.iterativeJoin(10, inputDataset, maidsWithDtCount, "maid", "left");
    //      inputDataset.join(maidsWithDtCount, Seq("maid"), "left").persist(StorageLevel.MEMORY_AND_DISK);
  }

  val doesTwoIntervalOverlap=(interval1:String,interval2:String)=>{
    val interv1=interval1.replace("+", "-"+Integer.MAX_VALUE)
    val interv2=interval2.replace("+", "-"+Integer.MAX_VALUE);

    val x1=interv1.split("-").apply(0).toInt
    val y1=interv1.split("-").apply(1).toInt;

    val x2=interv2.split("-").apply(0).toInt
    val y2=interv2.split("-").apply(1).toInt;

    if(x1>y1 || x2>y2)throw new Exception("Bad age_range start greater than end found in comparing age_ranges")
    if(x2>y1 || x1>y2) java.lang.Boolean.FALSE
    else java.lang.Boolean.TRUE;
  }

  def doesListContainNonOverlappingAgeRange(ageRangeList:Seq[String]):java.lang.Boolean={
    var isThereNonOverlap=java.lang.Boolean.FALSE
    for(i <-0 until ageRangeList.size){
      for(j <- 0 until ageRangeList.size){
        if(i!=j){
          isThereNonOverlap= !doesTwoIntervalOverlap(ageRangeList(i),ageRangeList(j));//||isThereNonOverlap;
        }
      }
    }
    isThereNonOverlap
  }
  def ageToAgeRange(age:Int):String={
    age match {
      case age1 if 0 to 12 contains age1 => "TOO YOUNG"
      case age2 if 13 to 17 contains age2 => "13-17"
      case age18 if 18 to 20 contains age18 => "18-20"
      case age3 if 21 to 24 contains age3 => "21-24"
      case age4 if 25 to 29 contains age4 => "25-29"
      case age5 if 30 to 34 contains age5 => "30-34"
      case age6 if 35 to 39 contains age6 => "35-39"
      case age7 if 40 to 44 contains age7 => "40-44"
      case age8 if 45 to 49 contains age8 => "45-49"
      case age9 if 50 to 54 contains age9 => "50-54"
      case age10 if 55 to 64 contains age10 => "55-64"
      case age11 if 65 to 69 contains age11 => "65-69"
      case age12 if 70 to 74 contains age12 => "70-74"
      case age13 if 75 to 90 contains age13 => "75+"
      case age14 if age14>90=> "TOO OLD"
      case _ => null;
    }
  }

  def inferAgeRange(yearOfBirth:String,monthOfBirth:String,dayOfBirth:String,age_range:String):String={
    var yearVerif=if(yearOfBirth!=null && yearOfBirth.toInt<1900)null else yearOfBirth;

    if(yearOfBirth==null  && age_range==null)return null;
    else if(yearOfBirth==null)return age_range;
    else{
      var year=yearOfBirth.toInt
      var month=Option(monthOfBirth).getOrElse("1").toInt
      var day=Option(dayOfBirth).getOrElse("1").toInt;
      val birthDate=LocalDateTime.of(year, month, day,0,0,0).atZone(ZoneId.of( "UTC" ));
      val now=LocalDateTime.now().atZone(ZoneId.of( "UTC" ));
      val age=Period.between(birthDate.toLocalDate(), now.toLocalDate()).getYears;
      return ageToAgeRange(age);
    }

  }


  def testAgeRangeConflict(sqlContext: SQLContext) {
    import sqlContext.implicits._

    val nonOverlappingAgeRangesUDF=udf((ageRangesList:Seq[String])=>{
      doesListContainNonOverlappingAgeRange(ageRangesList)
    })

    val iVal = Seq(("0", "13-17"), ("0", "18-20"), ("0", "21-24"),
      ("8", "13-17"), ("8", "18-20"), ("8", "21-24"),
      ("9", "13-17"), ("9", "18-20"), ("9", "21-24"),
      ("10", "13-17"), ("10", "18-20"), ("10", "21-24"));

      val idf = iVal.toDF("maid", "age_range")

    //val odf = iVal.toDF("maid", "age_range")

    val maidsWithOverlappingAgeRanges=idf.groupBy("maid")
      .agg(collect_set("age_range").as("age_ranges_list"))
      .withColumn("contains_non_overlapping_age_ranges", nonOverlappingAgeRangesUDF(col("age_ranges_list")))
      .filter("contains_non_overlapping_age_ranges=true")
      .persist(StorageLevel.MEMORY_AND_DISK);

    println("maidsWithOverlappingAgeRanges, count:"+maidsWithOverlappingAgeRanges.count())
    maidsWithOverlappingAgeRanges.show()
    idf.show()


    val demoWithNonOverlappARLabelAttached=idf.join(maidsWithOverlappingAgeRanges, maidsWithOverlappingAgeRanges("maid")===true,"left")//(maidsWithOverlappingAgeRanges,Seq("maid"),"left")

    println("demoWithNonOverlappARLabelAttached, count:"+ demoWithNonOverlappARLabelAttached.count())
    demoWithNonOverlappARLabelAttached.show()
  }



//  def getAgeRangeConflictResolverStage(spark:SparkSession):Unit={
//
//    val ageRangeLabelPreprocessor=(inputDataset:Dataset[Row])=>{
//      val cols="maid,cid,gender,language,age_range,ts,ip,country,dt,doe,source,last_seen_file_name,birth_day,birth_month,birth_year,dou"
//
//      val nonOverlappingAgeRangesUDF=udf((ageRangesList:Seq[String])=>{
//        doesListContainNonOverlappingAgeRange(ageRangesList)
//      })
//      val demoDF=inputDataset.select(cols.split(",").map(x=>col(x)):_*)
//      val maidsWithOverlappingAgeRanges=demoDF.groupBy("maid")
//        .agg(collect_set("age_range").as("age_ranges_list"))
//        .withColumn("contains_non_overlapping_age_ranges", nonOverlappingAgeRangesUDF(col("age_ranges_list")))
//        .filter("contains_non_overlapping_age_ranges=true")
//        .persist(StorageLevel.MEMORY_AND_DISK);
//      println("Count of maids with non overlapping age_ranges: "+maidsWithOverlappingAgeRanges.count());
//
//
//      val demoWithNonOverlappARLabelAttached=demoDF.join(maidsWithOverlappingAgeRanges,demoDF("maid"),"left")
//
//      demoWithNonOverlappARLabelAttached
//    }
//
//    val ageRangeConflictFunction=(contains_non_overlapping_age_ranges:java.lang.Boolean)=>{
//      if(contains_non_overlapping_age_ranges==null) java.lang.Boolean.TRUE
//      else java.lang.Boolean.FALSE;
//    }
//    val ageRangeConflictUDF=udf(ageRangeConflictFunction)
//    //val ageRangeLabelCountRule=CommonFieldsValidatorFactory.createValidator(Seq("contains_non_overlapping_age_ranges"), ageRangeConflictUDF, "CONFLICTING_AGE_RANGE_LABEL", "ERROR",false)
//    //val rules=Seq(ageRangeLabelCountRule)
//    //new Stage(rules,ageRangeLabelPreprocessor,()=>"Age Range Label Verifier")
//  }

  def makeContirbutorFile(): (Unit, Unit) = {
    var cid1Data: Seq[String] = Seq()
    var cid2Data: Seq[String] = Seq()
    var cid3Data: Seq[String] = Seq()

    var vcid1Data: Seq[String] = Seq()
    var vcid2Data: Seq[String] = Seq()
    var vcid3Data: Seq[String] = Seq()
    var ts = "";
    var j = 0;
    var k = 0;
    for (i <- 0 to 20) {
      val today = LocalDateTime.now()
      j = 1
      //4*0+1
      val maid1 = if (i == j) null else if (i == j + 1) StringUtils.repeat("0", 32) else if (i == j + 2) "" else if (i == j + 3) Util.makeMaid(i) else if (i == j + 4) StringUtils.repeat("nonAlpHa", 4) else if (i == j + 5) Util.makeMaid(i).substring(0, 16) else if (i == j + 6) Util.makeMaid(i).substring(0, 16) else Util.makeMaid(i)
      val maid2 = if (i == j) null else if (i == j + 1) StringUtils.repeat("0", 32) else if (i == j + 2) "" else if (i == j + 3) Util.makeMaid(i) else if (i == j + 4) StringUtils.repeat("nonAlpHa", 4) else if (i == j + 5) Util.makeMaid(i).substring(0, 16) else if (i == j + 6) Util.makeMaid(i).substring(0, 16) else Util.makeMaid(i)
      val maid3 = if (i == j) null else if (i == j + 1) StringUtils.repeat("0", 32) else if (i == j + 2) "" else if (i == j + 3) Util.makeMaid(i) else if (i == j + 4) StringUtils.repeat("nonAlpHa", 4) else if (i == j + 5) Util.makeMaid(i).substring(0, 16) else if (i == j + 6) Util.makeMaid(i).substring(0, 16) else Util.makeMaid(i)
      k = k + 2
      j = 4 * k + 1 //4*2+1

      // null,empty,nonhexa,lessthan expected length
      val cid1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) StringUtils.repeat("nonHexaa", 5) else if(i==j+3) "1" else if(i>8) "1" else "1"
      val cid2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) StringUtils.repeat("nonHexaa", 5) else if(i==j+3) "2" else if(i>8) "1" else "2"
      val cid3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) StringUtils.repeat("nonHexaa", 5) else if(i==j+3) "3" else if(i>8) "1" else "3"
      k = k + 1
      j = 4 * k + 1 //4*3+1

      val gender1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) StringUtils.repeat("nonHexaa", 4) else if(i==j+3) "F" else if(i>8) "M" else "M" //if(i==j+3) "M" else if(i==j+7) "F" else Util.makeGender(i)
      val gender2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) StringUtils.repeat("nonHexaa", 4) else if(i==j+3) "M" else if(i>8) "M" else "F" //if(i==j+3) "F" else if(i==j+7) "M" else Util.makeGender(i)
      val gender3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) StringUtils.repeat("nonHexaa", 4) else if(i==j+3) "F" else if(i>8) "M" else "M" //if(i==j+3) "M" else if(i==j+7) "F" else Util.makeGender(i)
      k = k + 1
      j = 4 * k + 1 //17//4*4+1

      val language1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "ar" else "en"
      val language2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "kr" else "en"
      val language3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "su" else "en"
      k = k + 1
      j = 4 * k + 1 //21//4*5+1

      val age_range1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "10-20" else "13-17"
      val age_range2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "30-40" else "18-20"
      val age_range3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "40-50" else "13-17" //"21-24"
      k = k + 1
      j = 4 * k + 1 //25//4*6+1

      // null, empty , future, and older than 18 months
      val ts1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) Util.makeTimestamp(today.getYear - 2, today.getMonthValue, today.getDayOfMonth, 0) else Util.makeTimestamp(today.getYear, today.getMonthValue, today.getDayOfMonth, 0)
      val ts2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) Util.makeTimestamp(today.getYear - 2, today.getMonthValue, today.getDayOfMonth, 0) else Util.makeTimestamp(today.getYear, today.getMonthValue, today.getDayOfMonth, 0)
      val ts3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) Util.makeTimestamp(today.getYear - 2, today.getMonthValue, today.getDayOfMonth, 0) else Util.makeTimestamp(today.getYear, today.getMonthValue, today.getDayOfMonth, 0)
      k = k + 1
      j = 4 * k + 1 //29//4*7+1

      val ip1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "12.345.a.234" else "10.10.10.10"
      //Util.makeIP(i)
      val ip2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "oyu.345.a.234" else "20.20.20.20"
      //Util.makeIP(i)
      val ip3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "1aa.345.c.234" else "30.30.30.30" //Util.makeIP(i)
      k = k + 1
      j = 4 * k + 1 //33//4*8+1

      val country1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) Util.makeWhiteListCountry(i) + "873" else "US" //Util.makeWhiteListCountry(i)
      val country2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) Util.makeWhiteListCountry(i) + "853" else "GB" //Util.makeWhiteListCountry(i)
      val country3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) Util.makeWhiteListCountry(i) + "813" else "FR" //Util.makeWhiteListCountry(i)
      k = k + 1
      j = 4 * k + 1 //4*10+1

      //null, empty, invalid, valid
      val dt1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "1" else "2"
      val dt2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "3" else "4"
      val dt3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "5" else "2"
      k = k + 1
      j = 4 * k + 1 //41//4*11+1

      //null, empty, invalid, valid
      val doe1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) Util.makeTimestamp(today.getYear - 2, today.getMonthValue, today.getDayOfMonth, 0) else Util.makeTimestamp(today.getYear, today.getMonthValue, today.getDayOfMonth, 0)
      val doe2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) Util.makeTimestamp(today.getYear - 2, today.getMonthValue, today.getDayOfMonth, 0) else Util.makeTimestamp(today.getYear, today.getMonthValue, today.getDayOfMonth, 0)
      val doe3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) Util.makeTimestamp(today.getYear - 2, today.getMonthValue, today.getDayOfMonth, 0) else Util.makeTimestamp(today.getYear, today.getMonthValue, today.getDayOfMonth, 0)
      k = k + 1
      j = 4 * k + 1 //45//4*12+1

      val source1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "tv" else "batch"
      val source2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "mobile" else "batch"
      val source3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "cloud" else "batch"
      k = k + 1
      j = 4 * k + 1 //49//4*13+1

      val last_seen_file_name1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "file1.csv" else "contib_1_demo_2018.csv"
      val last_seen_file_name2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "file2.csv" else "contib_2_demo_2018.csv"
      val last_seen_file_name3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "file3.csv" else "contib_3_demo_2018.csv"
      k = k + 1
      j = 4 * k + 1 //53//4*14+1

      val birth_day1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "32" else "15"
      val birth_day2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "33" else "16"
      val birth_day3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "34" else "17"
      k = k + 1
      j = 4 * k + 1 //57//4*15+1

      val birth_month1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "13" else "2"
      val birth_month2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "14" else "3"
      val birth_month3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "15" else "4"
      k = k + 1
      j = 4 * k + 1 //61//4*16+1

      val birth_year1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "100" else "2004"
      val birth_year2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "200" else "2000"
      val birth_year3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) "300" else "2004"//"1996"
      k = k + 1
      j = 4 * k + 1 //65//4*17+1

      val dou1 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) Util.makeTimestamp(today.getYear - 2, today.getMonthValue, today.getDayOfMonth, 0) else Util.makeTimestamp(today.getYear, today.getMonthValue, today.getDayOfMonth, 0)
      val dou2 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) Util.makeTimestamp(today.getYear - 2, today.getMonthValue, today.getDayOfMonth, 0) else Util.makeTimestamp(today.getYear, today.getMonthValue, today.getDayOfMonth, 0)
      val dou3 = if (i == j) null else if (i == j + 1) "" else if (i == j + 2) Util.makeTimestamp(today.getYear - 2, today.getMonthValue, today.getDayOfMonth, 0) else Util.makeTimestamp(today.getYear, today.getMonthValue, today.getDayOfMonth, 0)
      k = k + 1
      j = 4 * k + 1

      // cid2 delimiter is pipe, others are comma
      //maid,cid,gender,language,age_range,ts,ip,country,dt,doe,source,last_seen_file_name,birth_day,birth_month,birth_year,dou
      val row_cid1 = Seq(maid1, cid1, gender1, language1, age_range1, ts1, ip1, country1, dt1, doe1, source1, last_seen_file_name1, birth_day1, birth_month1, birth_year1, dou1).mkString(",")
      val row_cid2 = Seq(maid2, cid2, gender2, language2, age_range2, ts2, ip2, country2, dt2, doe2, source2, last_seen_file_name2, birth_day2, birth_month2, birth_year2, dou2).mkString("|")
      val row_cid3 = Seq(maid3, cid3, gender3, language3, age_range3, ts3, ip3, country3, dt3, doe3, source3, last_seen_file_name3, birth_day3, birth_month3, birth_year3, dou3).mkString(",")

      if ((i % 4 == 0) || (i>8)) {
        /*vdata = vdata ++ Seq(((maid1, cid1, gender1, language1, age_range1, ts1, ip1, country1, dt1, doe1, source1, last_seen_file_name1, birth_day1, birth_month1, birth_year1, dou1)),
          ((maid2, cid2, gender2, language2, age_range2, ts2, ip2, country2, dt2, doe2, source2, last_seen_file_name2, birth_day2, birth_month2, birth_year2, dou2)),
          ((maid3, cid3, gender3, language3, age_range3, ts3, ip3, country3, dt3, doe3, source3, last_seen_file_name3, birth_day3, birth_month3, birth_year3, dou3)))*/
        val row_vdata1 = Seq(maid1, cid1, gender1, language1, age_range1, ts1, ip1, country1, dt1, doe1, source1, last_seen_file_name1, birth_day1, birth_month1, birth_year1, dou1).mkString(",")
        val row_vdata2 = Seq(maid2, cid2, gender2, language2, age_range2, ts2, ip2, country2, dt2, doe2, source2, last_seen_file_name2, birth_day2, birth_month2, birth_year2, dou2).mkString("|")
        val row_vdata3 = Seq(maid3, cid3, gender3, language3, age_range3, ts3, ip3, country3, dt3, doe3, source3, last_seen_file_name3, birth_day3, birth_month3, birth_year3, dou3).mkString(",")

        vcid1Data = vcid1Data :+ row_vdata1
        vcid2Data = vcid2Data :+ row_vdata2
        vcid3Data = vcid3Data :+ row_vdata3
        //vCidData:+vdata.mkString("\n")
      }

      cid1Data = cid1Data :+ row_cid1
      cid2Data = cid2Data :+ row_cid2
      cid3Data = cid3Data :+ row_cid3

      ts = ts2;

      //var df = (maid1, cid1, gender1, language1, age_range1, ts1, ip1, country1, dt1, doe1, source1, last_seen_file_name1, birth_day1, birth_month1, birth_year1, dou1)
    }
    /*Special Cases*/
    /*DT Conflict*/
    //    val dtConflict_row1= Seq(Util.makeMaid(100),"1","F","en","18-20",ts,"40.10.10.10","US","2",ts,"batch","contib_1_demo_2018.csv","15","2","2004",ts).mkString(",")
    //    cid1Data=cid1Data :+ dtConflict_row1;
    //    vcid1Data=vcid1Data:+ dtConflict_row1;

    /*Age Range Conflict*/
    //    val ageRangeConflict_row1= Seq(Util.makeMaid(102),"1","F","en","18-20",ts,"50.10.10.10","US","4",ts,"batch","contib_1_demo_2018.csv","15","2","2004",ts).mkString(",")
    //    cid1Data=cid1Data :+ ageRangeConflict_row1;
    //    vcid1Data=vcid1Data:+ageRangeConflict_row1;
    //      val genderConflict_row = Seq("7fffffff7fffffff7fffffff7fffffff","1","M","en","18-20",ts,"10.10.10.10","US","2",ts,"batch,contib_1_demo_2018.csv","15","2","2004",ts).mkString(",")
    //      cid1Data=cid1Data :+ genderConflict_row;
    //      vcid1Data=vcid1Data:+genderConflict_row;

    //(Seq(cid1Data.mkString("\n"), cid2Data.mkString("\n"), cid3Data.mkString("\n")), /*vCidData)*/ Seq(vcid1Data.mkString("\n"), vcid2Data.mkString("\n"), vcid3Data.mkString("\n")))
    val oneDF = Seq(cid1Data.mkString("\n"))
    val twoDF = Seq(vcid1Data.mkString("\n"))//, cid2Data.mkString("\n"), cid3Data.mkString("\n"))
    (oneDF,twoDF)
  }




}

