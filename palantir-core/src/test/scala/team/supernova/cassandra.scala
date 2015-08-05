package team.supernova

import java.util.Properties

import com.datastax.driver.core.{ProtocolOptions, Session, Cluster}
//import scala.io.Source
//import java.io.File
import akka.actor.ActorSystem

trait TestCassandraCluster extends CassandraCluster {
  def system: ActorSystem
  import scala.collection.JavaConversions._

  val props: Properties = new Properties
  props.load(this.getClass.getClassLoader.getResourceAsStream("test.properties"))
  val username = props.getProperty("palantir.cassandra.username")
  val password = props.getProperty("palantir.cassandra.password")
  val port = props.getProperty("palantir.cassandra.port").toInt
  val keyspace = props.getProperty("palantir.cassandra.keyspace")
  val hosts = props.getProperty("palantir.cassandra.hosts").split(",").toList

//Using AKKA configuratuons
//  private def config = system.settings.config
//  private val cassandraConfig = config.getConfig("akka-cassandra.test.db.cassandra")
//  private val port = cassandraConfig.getInt("port")
//  private val username = cassandraConfig.getString("username")
//  private val password = cassandraConfig.getString("password")
//  private val keyspace = cassandraConfig.getString("keyspace")
//  private val hosts = cassandraConfig.getStringList("hosts").toList

  lazy val session: Session =
    Cluster.builder().
      addContactPoints(hosts: _*).
      withCompression(ProtocolOptions.Compression.SNAPPY).
      withPort(port).
      withCredentials(username, password).
      build().
      connect(keyspace)
}