import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics

//get review info
val reviews = sc.textFile("amazon/reviewsNew.txt").map(_.split("\t")).filter(_.size==8)
val reviews_info = reviews.map(line=>(line(7).trim(),(line(0).trim(),line(1).trim(),line(2).trim(),line(3).trim(),line(4).trim(),line(5).trim(),line(6).trim()))).
							filter(tp=>tp._1.length>1).
							filter(tp=>tp._2._3.length>1).
							filter(tp=>tp._2._7.length>1).
							filter(tp=>Try(tp._2._4.toDouble).isSuccess).
							filter(tp=>Try(tp._2._5.toDouble).isSuccess).
							filter(tp=>Try(tp._2._6.toDouble).isSuccess)
reviews_info.cache()
val unique_reviews = reviews_info.groupByKey()
val fake_reviews = unique_reviews.mapValues(_.size).filter(tp=>tp._2>1)
fake_reviews.cache()

//reviews_info
val fake_review_info = fake_reviews.join(reviews_info).mapValues(tp=>tp._2).zipWithIndex.map(tp=>(tp._2,(tp._1._1,tp._1._2,1)))
val fake_review_count = fake_review_info.count
val reviews_count = reviews_info.count

//subsample non-fake_reviews
val sample_rate = fake_review_count/reviews_count.toDouble
val none_fake_reviews = unique_reviews.mapValues(_.size).filter(tp=>tp._2==1).sample(false,sample_rate,12345)
val none_fake_reviews_info = none_fake_reviews.join(reviews_info).mapValues(tp=>tp._2).zipWithIndex.map(tp=>(tp._2+fake_review_count,(tp._1._1,tp._1._2,0)))

//merge two data
val review_data = fake_review_info.union(none_fake_reviews_info)
review_data.cache()

////user Info
val conf = new Configuration(sc.hadoopConfiguration)
conf.set("textinputformat.record.delimiter", "MEMBER INFO")
val dataset = sc.newAPIHadoopFile("amazon/amazon-member-shortSummary.txt", classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
val data = dataset.map(x=>x._2.toString)
val filter = data.map(line=>line.split("\t")).filter(l=>l.size>2)
val userInfo1 = filter.map(line=>(line(0).trim(),(line(1),line(2),line(3))))

val member = sc.textFile("amazon/amazon-memberinfo-locations.txt")
val filtered = member.map(line=>line.split("\t")).filter(strs=>strs.size==7)
val userInfo2 = filtered.map(line=>(line(5),(line(1),line(4))))

//product Info
val conf = new Configuration(sc.hadoopConfiguration)
conf.set("textinputformat.record.delimiter", "BREAK")
val dataset = sc.newAPIHadoopFile("amazon/productInfo.txt", classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
val data = dataset.map(x=>x._2.toString.replace("-REVIEWED","").trim())
val productInfo = data.map(line=>line.split("\t")).filter(l=>l.size>6).
					map(line=>(line(0),(line(1).trim(),line(3).trim(),line(4).trim(),line(5).trim())))

//features of reviews
//#length of body
val review_length = review_data.mapValues(tp=>tp._1.split(" ").size)

//#length of title
val title_length = review_data.mapValues(tp=>tp._2._7.size)

//#helpful
val num_helpful = review_data.mapValues(tp=>tp._2._4.toDouble)

//#feedback
val num_feedback = review_data.mapValues(tp=>tp._2._5.toDouble)

//rate
val rate = review_data.mapValues(tp=>tp._2._6.toDouble)

def count_cap_words(text:Array[String]): Double = {
	var count = 0
	val CapPattern = "[A-Z]+".r
	for(w<-text){
		if (CapPattern.findFirstIn(w).size>0)
			count+=1
	}
	return count.toDouble/text.size
}
//#cap/#word
val ratio_cap = review_data.mapValues(tp=>count_cap_words(tp._1.split(" ")))

//check using only review features 
val review_features = review_length.join(title_length).mapValues(tp=>List(tp._1.toDouble,tp._2)).
						join(num_helpful).mapValues(tp=>tp._1:+tp._2).
						join(num_feedback).mapValues(tp=>tp._1:+tp._2).
						join(rate).mapValues(tp=>tp._1:+tp._2).
						join(ratio_cap).mapValues(tp=>tp._1:+tp._2)

val train_label = review_data.mapValues(tp=>tp._3)

val data1 = train_label.join(review_features).map(tp=>LabeledPoint(tp._2._1.toDouble, Vectors.dense(tp._2._2.toArray).toSparse))
val splits = data1.randomSplit(Array(0.7, 0.3), seed = 11L)
val training = splits(0).cache()
val test = splits(1)


val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training)

// Compute raw scores on the test set.
val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
  val prediction = model.predict(features)
  (prediction, label)
}

// Get evaluation metrics.
val metrics = new MulticlassMetrics(predictionAndLabels)
val F1 = metrics.fMeasure


