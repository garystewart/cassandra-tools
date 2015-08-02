package team.supernova

import com.datastax.driver.core.{ProtocolOptions, Session, Cluster}
//import scala.io.Source
//import java.io.File
import akka.actor.ActorSystem

trait TestCassandraCluster extends CassandraCluster {
  def system: ActorSystem

  private def config = system.settings.config

  import scala.collection.JavaConversions._
  private val cassandraConfig = config.getConfig("akka-cassandra.test.db.cassandra")
  private val port = cassandraConfig.getInt("port")
  private val username = cassandraConfig.getString("username")
  private val password = cassandraConfig.getString("password")
  private val keyspace = cassandraConfig.getString("keyspace")
  private val hosts = cassandraConfig.getStringList("hosts").toList

  lazy val session: Session =
    Cluster.builder().
      addContactPoints(hosts: _*).
      withCompression(ProtocolOptions.Compression.SNAPPY).
      withPort(port).
      withCredentials(username, password).
      build().
      connect(keyspace)
}

/*
trait CleanCassandra extends SpecificationStructure {
  this: CassandraCluster =>

  private def runCql(session: Session, file: File): Unit = {
    val query = Source.fromFile(file).mkString
    println("executing: " + query)
    query.split(";").foreach(session.execute)
    println("finished: " + query)
  }

  private def runAllCqls(): Unit = {
    val uri = getClass.getResource("/").toURI
    new File(uri).listFiles().foreach { file =>
      if (file.getName.endsWith(".cql")) runCql(session, file)
    }
   // session.close()
  }

  override def map(fs: => Fragments) = super.map(fs) insert Step(runAllCqls())
}
*/