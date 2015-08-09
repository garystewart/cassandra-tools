package team.supernova

import org.json4s.jackson.JsonMethods._
import team.supernova.domain.{CassandraYaml, Login}

import scalaj.http._

/**
 * Created by Gary Stewart on 4-8-2015.
 *
 */
case class OpsTableInfo (tableName: String, avgDataSizeMB: Long, numberSSTables: Long) extends Ordered [OpsTableInfo] {
  def compare (that: OpsTableInfo) = {
    this.tableName.compareTo(that.tableName)
  }
}

case class OpsKeyspaceInfo (keyspaceName: String, opsTableInfoList: List[OpsTableInfo]) extends Ordered [OpsKeyspaceInfo] {
  def compare (that: OpsKeyspaceInfo) = {
    this.keyspaceName.compareTo(that.keyspaceName)
  }
}
case class OpsCenterNode (name: String, cassandra: CassandraYaml, opsKeyspaceInfoList: List[OpsKeyspaceInfo] )
case class OpsCenterClusterInfo (login: Login,
                                 name: String,
                                 nodes: List[OpsCenterNode]
                                  )


object OpsCenter {
  val readTimeout = 20000
  val connTimeout = 10000

//  //TODO not sure why i canot serialize it!
//  def getClusterNames (host: String, login: Login) : List[String]  = {
//    import spray.json._
//    import DefaultJsonProtocol._
//    val clusterConfigRespone = Http(s"http://$host/cluster-configs").header("opscenter-session", login.sessionid).header("Accept", "text/json").timeout(connTimeoutMs = connTimeout, readTimeoutMs = readTimeout).asString.body.parseJson
//    println (clusterConfigRespone)
//    //TODO - get more information
//    clusterConfigRespone.asJsObject().fields.map(i => {i._1}).toList
//  }


  //TODO not sure why i canot serialize it!
  def getNodeNames (host: String, login: Login, clusterName: String) : List[String]  = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._

    val nodesRes = Http(s"http://$host/$clusterName/nodes").header("opscenter-session", login.sessionid).header("Accept", "text/json").timeout(connTimeoutMs = 1000, readTimeoutMs = 10000).asString.body
    (parse(nodesRes) \\ "node_ip").children.map(_.values.toString)
  }


//  def getTableSize(login: Login, host: String, uname: String, pword: String, clusterName: String, keyspaceName: String) = {
//
//    val table_info = Http(s"http://$host/$clusterName/cluster-metrics/all/$keyspaceName/Users/cf-live-disk-used")
//      .header("opscenter-session", login.sessionid).header("Accept", "text/json")
//      .param("function","max").param("start",(System.currentTimeMillis()-10000000).toString).param("end",System.currentTimeMillis().toString)//.param("step","60")
//      .timeout(connTimeoutMs = connTimeout, readTimeoutMs = readTimeout).asString.body
//    println(table_info)
//  }

  def getTableSize(login: Login,
                   host: String,
                   uname: String,
                   pword: String,
                   clusterName: String,
                   listKeyspaceInfo: Map[String, List[String]],
                   node: String
                    ):  List[OpsKeyspaceInfo] = {

    val retVal = listKeyspaceInfo.foldLeft(List[OpsKeyspaceInfo]()){(a,keyspaceName) =>
      val k = keyspaceName._2.foldLeft(List[OpsTableInfo]()) { (c,tableName) =>

        c ++ List(OpsTableInfo(tableName,
          Math.round(getMetricTable(login, host, uname, pword, clusterName, node, keyspaceName._1, tableName, "cf-total-disk-used") / 1048576),
          getMetricTable(login, host, uname, pword, clusterName, node, keyspaceName._1, tableName, "cf-live-sstables").toLong )
        )
      }
      a ++ List(OpsKeyspaceInfo(keyspaceName._1, k ) )
    }
    retVal
  }


  def getMetricTable(login: Login,
                     host: String,
                     uname: String,
                     pword: String,
                     clusterName: String,
                     node: String,
                     keyspaceName: String,
                     tableName: String,
                     metricName: String ) : Double = {
    val metric = Http(s"http://$host/$clusterName/metrics/$node/${keyspaceName}/$tableName/$metricName?step=1440").header("opscenter-session", login.sessionid).header("Accept", "text/json").timeout(connTimeoutMs = 1000, readTimeoutMs = 10000).asString.body
    var retVal = (parse(metric) \\ "AVERAGE").children.map(_.values).tail.head.asInstanceOf[List[Double]](1)
    println (s"$clusterName.$keyspaceName.$tableName $metricName on $node = $retVal ")
    retVal
  }


  //TODO check for more ideas - http://docs.datastax.com/en/opscenter/5.1/api/docs/index.html#
  def createOpsCenterClusterInfo (host: String,
                                  uname: String,
                                  pword: String,
                                  clusterName: String,
                                  listKeyspaceInfo: Map[String, List[String]]  //keyspaceNmae List[Tables]
                                   ): Option[OpsCenterClusterInfo] = {
    try {

      //login to OpsCenter and get session id
      val resultLogin = Http(s"http://$host/login").param("username", uname).param("password", pword).timeout(connTimeoutMs = connTimeout, readTimeoutMs = readTimeout).asString.body
      val login = Login.parseLogin(resultLogin)


      //TODO - finish off
      //val nodesRes = Http(s"http://$host/$clusterName/nodes").header("opscenter-session", login.sessionid).header("Accept", "text/json").timeout(connTimeoutMs = connTimeout, readTimeoutMs = readTimeout).asString.body
      //val nodes = Nodes.parseBody(nodesRes)

      val listNodeIP = getNodeNames(host, login, clusterName)
      println(s"$clusterName found nodes: $listNodeIP")
      //per node
      val listNodes = listNodeIP.map(node_ip => {
        val nodeIPres = Http(s"http://$host/$clusterName/nodeconf/$node_ip").header("opscenter-session", login.sessionid).header("Accept", "text/json").timeout(connTimeoutMs = connTimeout, readTimeoutMs = readTimeout).asString.body
        val keyInfo = getTableSize(login,host, uname, pword, clusterName,listKeyspaceInfo, node_ip )
        new OpsCenterNode(node_ip, CassandraYaml.parseBody(nodeIPres), keyInfo)
      })

      Some(new OpsCenterClusterInfo(login, clusterName, listNodes))
    }
    catch {case e: Exception => {
      println (s"$e")
      println (s"Failed to get OpsCenterInfo for $clusterName")
      None
    }}
  }
}





//TODO - ClusterName now input!  - This code can be used to find unknown cluster :-)
//    //find list of clusters
//    //TODO - val clusterCOnfig = ClusterConfig.parseClusterConfig(clusterConfigRespone)
//    val listClusterName= getClusterNames (host, login)
//    println (listClusterName)
