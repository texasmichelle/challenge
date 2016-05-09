import java.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.collection.mutable.ListBuffer
import scala.io.Source

object ChallengeApp {
  def main(args: Array[String]): Unit = {

    val cpuCores = Runtime.getRuntime().availableProcessors()

    val conf = new SparkConf().setAppName("Interview Challenge App")
      .set("spark.driver.host", "localhost")
      .setMaster(s"local[$cpuCores]")

    val sc = new SparkContext(conf)
    //val sc = new SparkContext("local", "Interview Challenge App", new SparkConf())

    try {
      val stopwords = getStopwords(sc, "resources/stopwords.txt")
      
      generateFeature1(sc, "resources/OANC-GrAF/data/spoken/telephone/switchboard/*/*.txt", stopwords)

      generateFeature2(sc, "resources/OANC-GrAF/data/spoken/telephone/switchboard/*/*.txt", stopwords)

    } finally {
      sc.stop()      // Stop (shut down) the context.
    }
  }

  def getStopwords(sc: SparkContext, filename: String) : Set[String] = {
      // Create an RDD of stopwords
      val stopwordsRdd = sc.textFile(filename)

      stopwordsRdd.collect.toSet
  }

  def generateFeature1(sc: SparkContext, inputPath: String, stopwords: Set[String]) = {
      // Create an RDD, one line per record
      val transcript = sc.textFile(inputPath)
      
      // Word count
      transcript.flatMap(line => line.split("\\s+")).map(word => (word.toLowerCase, 1)).reduceByKey((a, b) => a + b)
      // Remove stopwords
                .filter{case (word, count) => !stopwords.contains(word)}
      // Remove empty words
                .filter{case (word, count) => word.length > 0}
      // Sort by key
                .sortByKey(true, 1)
      // Format it properly for file output
                .map{case (word, count) => count + " " + word}
      // Write to a file
                .saveAsTextFile("output/feature1")

      // Move the output file
      new File("output/feature1/part-00000").renameTo(new File("output/feature1.txt"))

      // TODO: Remove intermediate files
  }

  def generateFeature2(sc: SparkContext, inputPath: String, stopwords: Set[String]) = {

      // Create an RDD with filename, full file content per record
      val transcript = sc.wholeTextFiles(inputPath)
      
      // Unique words per filename
      transcript.flatMap{case (filename, contents) => contents.split("\\s+").map(_.toLowerCase() + ";" + filename -> filename)}.reduceByKey((a, b) => a)
      // Remove stopwords
                .filter{case (word, filename) => !stopwords.contains(word.substring(0, word.indexOf(";")))}
      // Remove empty words
                .filter{case (word, filename) => word.substring(0, word.indexOf(";")).length > 0}
      // Number of unique words per file
                .map{ case (word, filename) => (filename, 1)}.reduceByKey((a, b) => a + b)
      // Sort in alphabetical order by filename
                .sortByKey(true, 1)
      // Format it properly for file output
                .map{case (filename, count) => count + " " + filename}
      // Write to a file
                .saveAsTextFile("output/feature2")
      
      // Process the file we just created to calculate the moving median
      val file = new File("output/feature2.txt")
      val bw = new BufferedWriter(new FileWriter(file))
      
      var word_counts = new ListBuffer[Int]()
      for(line <- Source.fromFile("output/feature2/part-00000").getLines) {
        // Only grab the integer & ignore the filename
      	word_counts += line.split(" ").head.toInt

      	var median = 0
      	// If there is an odd number, take the middle value
      	if (word_counts.length % 2 == 1) {
      		median = word_counts.sorted.drop(word_counts.length / 2).head
      	// If there is an even number, average the middle two values
      	} else {
      		val word_counts_sorted = word_counts.sorted
      		median = (word_counts_sorted(word_counts.length / 2 - 1) + word_counts_sorted(word_counts.length / 2)) / 2
      	}

        // This is the number we care about, so save it
      	bw.write(median + "\n")
      }
      bw.close()

      // TODO: Remove intermediate files
  }

}

