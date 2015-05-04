package ganda

import com.datastax.driver.core.{Session, ProtocolOptions, Cluster}
import akka.actor.ActorSystem

trait CassandraCluster {
  def session: Session
}

trait ConfigCassandraCluster extends CassandraCluster {
  def system: ActorSystem

  private def config = system.settings.config

  import scala.collection.JavaConversions._
  private val cassandraConfig = config.getConfig("akka-cassandra.main.db.cassandra")
  private val port = cassandraConfig.getInt("port")
  private val username = cassandraConfig.getString("username")
  private val password = cassandraConfig.getString("password")
  //no need to connect to keyspace - yet
  //private val keyspace = cassandraConfig.getString("keyspace")
  private val hosts = cassandraConfig.getStringList("hosts").toList

  lazy val session: Session =
    Cluster.builder().
      addContactPoints(hosts: _*).
      withCompression(ProtocolOptions.Compression.SNAPPY).
      withCredentials(username, password).
      withPort(port).
      build().
      //no need to connect to keyspace - yet
      connect()
}