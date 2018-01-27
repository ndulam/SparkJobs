object CassandraTable {

  def main(args: Array[String]): Unit =
	  {
		  val conf = new SparkConf(true)
		  conf.set("spark.cassandra.connection.host","localhost")
		  val sc = new SparkContext("local[2]","test",conf)
		  val badges = sc.cassandraTable[com.naresh.org.badgesclass]("stackoverflow","badges").select("id" as "id","class" as "cls","date" as "date","name" as "name","tagbased" as "tagbased","userid" as "userid")
		  val ids = sc.parallelize(1 to 1000).map(com.naresh.org.Customer(_));
		  val repartitined = ids.repartitionByCassandraReplica("stackoverflow","posts")
		  println(repartitined.partitions.size)
  }
}
