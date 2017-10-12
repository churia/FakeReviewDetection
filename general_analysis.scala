import scala.util.Try
//######User-Review statistics########
//1. histogram of #reviews/user
val reviews = sc.textFile("amazon/reviewsNew.txt").map(_.split("\t")).filter(_.size==8)
val user_reviews = reviews.map(line=>(line(0),line.slice(1,8)))
user_reviews.cache()
val reviews_count = user_reviews.count()
val users_review_count = user_reviews.groupByKey().mapValues(_.size)
val histogram = users_review_count.map(tp=>(tp._2,1)).reduceByKey(_+_).sortByKey()
histogram.saveAsTextFile("amazon/user_review_hist/")

//######Prodcut-Review/Rate statistics####
//1. histogram of #reviews/product
val product_reviews = reviews.map(line=>(line(1),1))
val product_review_count = product_reviews.reduceByKey(_+_)
val histogram = product_review_count.map(tp=>(tp._2,1)).reduceByKey(_+_).sortByKey()
histogram.saveAsTextFile("amazon/product_review_hist/")

//######Product/Review - Rating Statistics#####
//1. histogram of avg_rating/product
val product_rates = reviews.map(line=>(line(1),line(5))).filter(tp=>Try(tp._2.toDouble).isSuccess).mapValues(_.toDouble)
val valid_rates_count = product_rates.count()
val product_rates_avg = product_rates.groupByKey().mapValues(x=>x.sum/x.size)
val histogram = product_rates_avg.map(tp=>(tp._2,1)).reduceByKey(_+_).sortByKey()
histogram.saveAsTextFile("amazon/product_rates_hist/")

//2. histogram of all Ratings
val rates = reviews.map(line=>(line(5),1))
val rates_count = rates.reduceByKey(_+_)
val histogram = rates_count.sortByKey()
histogram.saveAsTextFile("amazon/rates_hist/")

//#####Review Statistics#####
val review_length = reviews.map(line=>(line(7).split(" ").size,1)) //word_count
val review_length_count = review_length.reduceByKey(_+_)
val histogram = review_length_count.sortByKey()
histogram.saveAsTextFile("amazon/review_length_hist/")
//To be set histogram interval
//To be plotted
