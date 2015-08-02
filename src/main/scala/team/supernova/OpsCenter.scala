package team.supernova

import team.supernova.domain.{CassandraYaml, Login}

import scalaj.http._

case class OpsCenterNode (name: String, cassandra: CassandraYaml )
case class OpsCenterClusterInfo (login: Login, name: String, nodes: List[OpsCenterNode])
//case class OpsCenter (login: Login, opsCenterHost: String, clusters: OpsCenterClusterInfo) {}


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





  def getTableSize(login: Login, host: String, uname: String, pword: String, clusterName: String, keyspaceName: String) = {

    val table_info = Http(s"http://$host/$clusterName/cluster-metrics/all/$keyspaceName/Users/cf-live-disk-used")
      .header("opscenter-session", login.sessionid).header("Accept", "text/json")
      .param("function","max").param("start",(System.currentTimeMillis()-10000000).toString).param("end",System.currentTimeMillis().toString)//.param("step","60")
      .timeout(connTimeoutMs = connTimeout, readTimeoutMs = readTimeout).asString.body
    println(table_info)
  }


  //TODO check for more ideas - http://docs.datastax.com/en/opscenter/5.1/api/docs/index.html#
  def createOpsCenterClusterInfo (host: String, uname: String, pword: String, clusterName: String): Option[OpsCenterClusterInfo] = {
    try {
      //login to OpsCenter and get session id
      val resultLogin = Http(s"http://$host/login").param("username", uname).param("password", pword).timeout(connTimeoutMs = connTimeout, readTimeoutMs = readTimeout).asString.body
      val login = Login.parseLogin(resultLogin)


      val nodesRes = Http(s"http://$host/$clusterName/nodes").header("opscenter-session", login.sessionid).header("Accept", "text/json").timeout(connTimeoutMs = connTimeout, readTimeoutMs = readTimeout).asString.body
      //TODO - val nodes = Nodes.parseBody(nodesRes)
      val listNodeIP = getNodeNames(host, login, clusterName)
      println(s"$clusterName found nodes: $listNodeIP")
      //per node
      val listNodes = listNodeIP.map(node_ip => {
        val nodeIPres = Http(s"http://$host/$clusterName/nodeconf/$node_ip").header("opscenter-session", login.sessionid).header("Accept", "text/json").timeout(connTimeoutMs = connTimeout, readTimeoutMs = readTimeout).asString.body
        new OpsCenterNode(node_ip, CassandraYaml.parseBody(nodeIPres))
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
