import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Random

import org.apache.commons.lang3.StringUtils

object Util {
  def main(args:Array[String]):Unit={
    val goodTime=LocalDateTime.now().minusDays(2);
    println(Util.makeDate(goodTime.getYear, goodTime.getMonthValue,goodTime.getDayOfMonth()));
    println(LocalDateTime.of(2018, 12, 25, 0, 0,0).atZone(ZoneId.of("UTC")));
    println(LocalDateTime.now().minusDays(2))

  }
  def makeDate(year:Int,month:Int,dayOfMonth:Int,hour:Int=0,minute:Int=0,sec:Int=0):String={
    LocalDateTime.of(year, month, dayOfMonth, hour, minute,sec).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:SS"))
    //LocalDateTime.of(year, month, dayOfMonth, hour, minute,sec).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:MM:SS"))
  }
  def makeWhiteListCountry(id:Int):String={
    val countryList=List("US","GB","FR"/*,"AT","AX","BE","BG","CN","CY","CZ","DE","DK","EE","ES","FI","FO"*/);
    val index=id%countryList.size;
    countryList.apply(index);
  }
  def makeBlackListCountry(id:Int):String={
    val countryList=List("ET","EG","ER","SO");
    val index=id%countryList.size;
    countryList.apply(index);
  }
  def makeIP(id:Int):String={
    val random=new Random(id);
    return (random.nextInt(5)+250)+"."+(random.nextInt(5)+250)+"."+(random.nextInt(5)+250)+"."+(random.nextInt(5)+250);
  }
  def makeTimestamp(year:Int,month:Int,dayOfMonth:Int,hour:Int=0,minute:Int=0,sec:Int=0):String={
    val localDate=LocalDateTime.of(year, month, dayOfMonth, hour, minute,sec);
    val epoch = localDate.atZone(ZoneId.of("UTC")).toEpochSecond();
    epoch+"000";
  }
  def make16DigitHash(id:Int):String={
    toBase16(id,16);
  }
  def make16DigitMaid(id:Int):String={
    toBase16(id,16).toUpperCase();
  }
  def makeMaid(id:Int):String={
    toBase16(id,32).toUpperCase();
  }
  def makeEmailSha1(id:Int):String={
    toBase16(id,40);
  }
  def makeAPIKey(id:Int):String={
    toBase16(id,32);
  }
  def makeEmailMd5(id:Int):String={
    toBase16(id,32);
  }
  def makeEmailSha256(id:Int):String={
    toBase16(id,64);
  }
  /**
    * len must be multiple of 8
    */
  def toBase16(num:Int,len:Int):String={
    val repeat=len/8;
    val ran=new Random(10000000).nextInt(10000000)
    val numModif= Integer.MAX_VALUE-num;
    val hex = Integer.toHexString(numModif);
    val bStr=StringUtils.leftPad(hex, 8,"0");
    StringUtils.repeat(bStr, repeat)
  }
  def makeAge(id:Int):String={
    val random = new Random(id)
    if(random.nextInt(100) == 0) "20" else random.nextInt(100).toString
  }
  def makeGender(id:Int):String={
    val genderList=List("M","m","F","f","Male","male","Female","female","mAle","fEMale");
    val index=id%genderList.size;
    genderList.apply(index);
  }

  def makeAgeRange(id:Int):String={
    val input = makeAge(id).toInt;

    input match{
      case age0 if 0 to 12 contains age0 => "TOO YOUNG"
      case age1 if 13 to 17 contains age1 => "13-17"
      case age2 if 18 to 20 contains age2 => "18-20"
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
      case _ => null
    }
    //"13-17"->true,"18-20"->true,"21-24"->true,"25-29"->true,"30-34"->true,"35-39"->true,"40-44"->true,"45-49"->true,"50-54"->true,"55-64"->true,"65-69"->true,"70-74"->true,"75+
  }


}
