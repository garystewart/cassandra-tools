package ganda

import scalaj.http._
import _root_.domain.{Nodes, Login, CassandraYaml}

case class OpsCenterNode (name: String, cassandra: CassandraYaml )
case class OpsCenterClusterInfo (name: String, nodes: List[OpsCenterNode])
case class OpsCenter (login: Login, opsCenterHost: String, clusters: List[OpsCenterClusterInfo]) {}


object OpsCenter {
  val readTimeout = 20000
  val connTimeout = 10000

  //TODO not sure why i canot serialize it!
  def getClusterNames (host: String, login: Login) : List[String]  = {
    import spray.json._
    import DefaultJsonProtocol._
    val clusterConfigRespone = Http(s"http://$host/cluster-configs").header("opscenter-session", login.sessionid).header("Accept", "text/json").timeout(connTimeoutMs = connTimeout, readTimeoutMs = readTimeout).asString.body.parseJson
    println (clusterConfigRespone)
    //TODO - get more information
    clusterConfigRespone.asJsObject().fields.map(i => {i._1}).toList
  }


  //TODO not sure why i canot serialize it!
  def getNodeNames (host: String, login: Login, clusterName: String) : List[String]  = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    //import play.api.libs.json.JsValue

    val nodesRes = Http(s"http://$host/$clusterName/nodes").header("opscenter-session", login.sessionid).header("Accept", "text/json").timeout(connTimeoutMs = 1000, readTimeoutMs = 10000).asString.body
    //println (nodesRes)
    (parse(nodesRes) \\ "node_ip").children.map(_.values.toString)
  }


  //TODO check for more ideas - http://docs.datastax.com/en/opscenter/5.1/api/docs/index.html#
  def createOpsCenter (host: String, uname: String, pword: String): OpsCenter = {

  //login to OpsCenter and get session id
  val resultLogin = Http(s"http://$host/login").param("username",uname).param("password",pword).timeout(connTimeoutMs = connTimeout, readTimeoutMs = readTimeout).asString.body
  val login = Login.parseLogin(resultLogin)

  //find list of clusters
  //TODO - val clusterCOnfig = ClusterConfig.parseClusterConfig(clusterConfigRespone)
  val listClusterName= getClusterNames (host, login)
  println (listClusterName)


  //per cluster
  val opsCenterClusterInfo = listClusterName.map(clusName => {
      val nodesRes = Http(s"http://$host/$clusName/nodes").header("opscenter-session", login.sessionid).header("Accept", "text/json").timeout(connTimeoutMs = connTimeout, readTimeoutMs = readTimeout).asString.body
      //TODO - val nodes = Nodes.parseBody(nodesRes)
      val listNodeIP= getNodeNames (host, login, clusName)
      println (listNodeIP)
      //per node
    val listNodes = listNodeIP.map(node_ip => {
      val nodeIPres = Http(s"http://$host/$clusName/nodeconf/$node_ip").header("opscenter-session", login.sessionid).header("Accept", "text/json").timeout(connTimeoutMs = connTimeout, readTimeoutMs = readTimeout).asString.body
      new OpsCenterNode (node_ip, CassandraYaml.parseCassandraYaml(nodeIPres))
    })
    new OpsCenterClusterInfo(clusName, listNodes)
  })

   new OpsCenter(login, host, opsCenterClusterInfo)
  }
}





//import spray.json._
////import DefaultJsonProtocol._
//
////case class OpsClusterInfo (name: String, properties: JsValue, nodes: JsValue) {
//case class OpsClusterInfo (name: String) {
//}
//
//


//
//  //TODO change to json4s
//  def getClusterConfigs (): List[OpsClusterInfo] = {
//    //val response: HttpResponse[String] = Http(s"http://$host/cluster-configs").header("opscenter-session", sessionId).header("Accept", "text/json").asString
//    val clusterConfigRespone = Http(s"http://$host/cluster-configs").header("opscenter-session", sessionId).header("Accept", "text/json").asString.body.parseJson
//
//    //per cluster
//    clusterConfigRespone.asJsObject.fields.map(i => {
//    //OpsCenter.filterField(parse(clusterConfigRespone), "node_ip").foreach ( i=> {
//      val clusterName =i._1
//      val nodesRes = Http(s"http://$host/$clusterName/nodes").header("opscenter-session", sessionId).header("Accept", "text/yaml").asString.body
//      //val nodesResponseBody =  "{\"nodes\": "  +res + "}"
//      OpsCenter.filterField(parse(nodesRes), "node_ip").foreach(a => {
//        val node_ip = a._2.values
//        println(s"Node found: $node_ip")
//        val nodeIPres = Http(s"http://$host/$clusterName/nodeconf/$node_ip").header("opscenter-session", sessionId).header("Accept", "text/json").asString.body
//
//        println (CassandraYaml.parseCassandraYaml(nodeIPres))
//
//        println (nodeIPres)
//        OpsCenter.filterField(parse(nodeIPres), "concurrent_writes").foreach(b => println (" writes: " + b._2.values))
//        import org.json4s._
//        //import org.json4s.native.JsonMethods._
//        import org.json4s.JsonDSL._
//        println ((parse(nodeIPres) \\ "concurrent_writes")(0).values)
//
//      })
//
//      val storageRespone = Http(s"http://$host/$clusterName/storage-capacity").header("opscenter-session", sessionId).header("Accept", "text/json").asString.body.parseJson
//      println (clusterName +" " + storageRespone)
//
//      new OpsClusterInfo(clusterName)// , i._2, clusterConfigRespone)
//    }).toList
//  }
//
//
//}


//  def filterField (json:  json4s.JValue, fieldName: String) = {
//    json.filterField {
//      case JField(`fieldName`, _) => true
//      case _ => false
//    }
//  }


