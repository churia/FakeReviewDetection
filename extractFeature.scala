import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint

//load review data, seprate info and text 
val reviewsInfo = sc.textFile("amazon/small_reviews.txt")
val temp = reviewsInfo.map(_.split("\t"))
val Info = temp.map(v=>((v(0),v(1)),v.slice(2,7)))
val text = temp.map(v=>((v(0),v(1)),v(7)))
//generate TFIDF feature
//TODO load label:(id, label)
//val instance ＝LabeledPoint(
val documents = text.map(tp=>tp._2.trim().split("[\\p{Punct}\\s]+").toSeq)
val hashingTF = new HashingTF()
val tf: RDD[Vector] = hashingTF.transform(documents)
tf.cache()
val idf = new IDF().fit(tf)
val tfidf: RDD[Vector] = idf.transform(tf)
tfidf.cache()


