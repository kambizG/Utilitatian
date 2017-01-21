
//####################################################################################################
// Temporal Distribution of tweets in different PostBox areas
//####################################################################################################
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val podate = sc.textFile("tw_lo_full_info_sortedByPoBox.txt").map(x => (x.split(",")(5), dateparser.parse(x.split(",")(2)).getHours()))
// (String, Int) = (BR1,22)
val area_hour_count = podate.map(x => (x, 1)).reduceByKey((a,b) => a+b).map(x => (x._1._1, (x._1._2, x._2)))
// (String, (Int, Int)) = (SE13,(5,478)) 
val area_count = podate.map(x => (x._1, 1)).reduceByKey((a,b) => a+b)
// (String, Int) = (SW17,61283)
val area_hour_ratio = area_hour_count.join(area_count).map(x => (x._1, (x._2._1._1, x._2._1._2 * 1.0 / x._2._2 * 1.0)))
// (String, (Int, Double)) = (SW17,(19,0.08101757420491816))

//scala> area_count.count
//res9: Long = 284

//####################################################################################################
// Temporal Distribution of tweets in different PostBox during WeekEnds and WeekDays => {0, 1, 2, 3, 4, 5, 6} = {sun, mon, tue, wed, thr, fri, sat}
//####################################################################################################
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val podate = sc.textFile("tw_lo_full_info_sortedByPoBox.txt").map(x => (x.split(",")(5), dateparser.parse(x.split(",")(2))))
// (String, java.util.Date) = (BR1,Sat Jun 14 22:22:16 CEST 2014)

// WeekEnds --------------------
val po_hour_weekEnd = podate.filter(x => x._2.getDay() == 0 || x._2.getDay() == 6).map(x => (x._1, x._2.getHours()))
// (String, Int) = (BR1,7)
val area_hour_count = po_hour_weekEnd.map(x => (x, 1)).reduceByKey((a,b) => a+b).map(x => (x._1._1, (x._1._2, x._2)))
val area_count = po_hour_weekEnd.map(x => (x._1, 1)).reduceByKey((a,b) => a+b)
val area_hour_ratio = area_hour_count.join(area_count).map(x => (x._1 + "," + x._2._2 + "," + x._2._1._1 + "," + x._2._1._2 * 1.0 / x._2._2 * 1.0))
area_hour_ratio.coalesce(1).saveAsTextFile("temp_week_end")
// WeekDays --------------------
val po_hour_weekDay = podate.filter(x => x._2.getDay() != 0 && x._2.getDay() != 6).map(x => (x._1, x._2.getHours()))
// (String, Int) = (BR1,7)
val area_hour_count = po_hour_weekDay.map(x => (x, 1)).reduceByKey((a,b) => a+b).map(x => (x._1._1, (x._1._2, x._2)))
val area_count = po_hour_weekDay.map(x => (x._1, 1)).reduceByKey((a,b) => a+b)
val area_hour_ratio = area_hour_count.join(area_count).map(x => (x._1 + "," + x._2._2 + "," + x._2._1._1 + "," + x._2._1._2 * 1.0 / x._2._2 * 1.0))
area_hour_ratio.coalesce(1).saveAsTextFile("temp_week_day")

//####################################################################################################
// Extract temporal-area distribution for specific area EC3V during weekend - because of the anomalous behavior
//####################################################################################################
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val podate = sc.textFile("tw_lo_full_info_sortedByPoBox.txt").map(x => (x.split(",")(5), x.split(",", 7)(1), dateparser.parse(x.split(",")(2)).getDay))
val temp = podate.filter(x => x._1 == "EC3V").filter(x => x._3 == 0 || x._3 == 6)

//####################################################################################################
// Extract Uniq Mutual User IDS
//####################################################################################################
val fr = sc.textFile("friends.txt").map(x => (x.split("\t")(0).toLong, x.split("\t")(1).toLong))
val fr_rev = fr.map(x => (x._2, x._1))
val fr_mut = fr.union(fr_rev)
val mutualFriends = fr_mut.map(x => (x, 1)).reduceByKey((a,b) => a+b).filter(x => x._2 > 1).map(x => x._1).filter(x => x._1 > x._2)
val uniqMutualUserIds = mutualFriends.map(x => x._1).union(mutualFriends.map(x => x._2)).map(x => (x, 1)).reduceByKey((a,b) => a+b).map(x => x._1)
uniqMutualUserIds.saveAsTextFile("uniqMutualUserIds")

//####################################################################################################
// Temporal Distribution of tweets in different PostBox areas on for "MUTUAL FRIENDS"
//####################################################################################################
val uniqMutualUserIds = sc.textFile("umi_sorted.txt").map(x => (x.toLong, 1))
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val data = sc.textFile("tw_lo_full_info_sortedByPoBox.txt").map(x => (x.split(",")(1).toLong, (x.split(",")(5), dateparser.parse(x.split(",")(2)))))
val podatemutual = uniqMutualUserIds.join(data)
val week_area_hour = podatemutual.map(x => x._2._2).map(x => (x._1, x._2.getHours))
val week_area_hour_count = week_area_hour.map(x => (x, 1)).reduceByKey((a,b) => a+b).map(x => (x._1._1, (x._1._2, x._2)))
val week_area_count = week_area_hour.map(x => (x._1, 1)).reduceByKey((a,b) => a+b)
val week_area_hour_ratio = week_area_hour_count.join(week_area_count).map(x => (x._1 + "," + x._2._2 + "," + x._2._1._1 + "," + x._2._1._2 * 1.0 / x._2._2 * 1.0))
week_area_hour_ratio.coalesce(1).saveAsTextFile("week_area_hour_ratio")

//####################################################################################################
//####################################################################################################

