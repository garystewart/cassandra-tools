package team.supernova

import java.util.Properties
import com.datastax.driver.core.{ProtocolOptions, Session, Cluster}
import akka.actor.ActorSystem


trait TestCassandraCluster  {
  def system: ActorSystem
  import scala.collection.JavaConversions._

  val props: Properties = new Properties
  props.load(this.getClass.getClassLoader.getResourceAsStream("test.properties"))
  val username = props.getProperty("palantir.cassandra.username")
  val password = props.getProperty("palantir.cassandra.password")
  val port = props.getProperty("palantir.cassandra.port").toInt
  val keyspace = props.getProperty("palantir.cassandra.keyspace")
  val hosts = props.getProperty("palantir.cassandra.hosts").split(",").toList

  lazy val session: Session =
    Cluster.builder().
      addContactPoints(hosts: _*).
      withCompression(ProtocolOptions.Compression.SNAPPY).
      withPort(port).
      withCredentials(username, password).
      build().
      connect(keyspace)
}