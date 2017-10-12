import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

val reviews = sc.textFile("amazon/reviewsNew.txt").map(_.split("\t")).filter(_.size==8)
val reviews_info = reviews.map(line=>(line(7).trim(),line.slice(0,7)))
reviews_info.cache()
//extactly same/duplicate
val reviews_count = reviews_info.count()
val unique_reviews = reviews_info.groupByKey()
val same_reviews = unique_reviews.mapValues(_.size).filter(tp=>tp._2>1)
val same_count = same_reviews.count()
//check top frequent same reviews
//val top_f = same_reviews.map(tp=>(tp._2,tp._1)).sortByKey(false)
//top_f.take(10).foreach(println)

//check top longest same reviews
val top_l = same_reviews.map(tp=>(tp._1.split(" ").size,tp._2)).sortByKey(false)
top_l.saveAsTextFile("amazon/save_review_length")
//top_l.take(10).foreach(println)


//similar/near-duplicate
//Fail to implement because of memory limit. To Do in the future

//analyze fake review features
//fake review Info
val fake_review_info = same_reviews.join(reviews_info)
fake_review_info.cache()
val fake_again = fake_review_info.mapValues(tp=>(tp._2(0),tp._2(1))).groupByKey
def dig_fake(info:Iterable[(String, String)]):Int={
	var pset = List[String]()
	var uset = List[String]()
	for((u,p)<-info){
		pset = pset:+p
		uset = uset:+u
	}
	if (uset.distinct.size==1 && pset.distinct.size==1)
		return 0
	else if (uset.distinct.size==1)
		return 1
	else if (pset.distinct.size==1)
		return 2
	else
		return 3
}
//fake review distribution in four kinds
//#reviews on same product by same user
val same_fake = fake_again.mapValues(x=>dig_fake(x)).filter(tp=>tp._2==0).count
//#reviews that written by same user on different products
val same_user_fake = fake_again.mapValues(x=>dig_fake(x)).filter(tp=>tp._2==1).count
//#reviews that written by different user on same products 
val same_product_fake = fake_again.mapValues(x=>dig_fake(x)).filter(tp=>tp._2==2).count
//rest
val rest_fake = fake_again.mapValues(x=>dig_fake(x)).filter(tp=>tp._2==3).count

