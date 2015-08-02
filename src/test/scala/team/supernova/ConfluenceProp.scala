package team.supernova

import java.util.Properties

import nl.ing.confluence.rpc.soap.actions.Token

object ConfluenceProp {
  case class confluenceCredentials(username: String, password: String )

  def getConfluenceCredentials: confluenceCredentials  = {
    val props: Properties = new Properties
    props.load(this.getClass.getClassLoader.getResourceAsStream("test.properties"))
    val confluenceUser = props.getProperty("confluence.user")
    val confluencePassword = props.getProperty("confluence.password")
    confluenceCredentials (confluenceUser, confluencePassword)
  }

  def getConfluenceToken: Token = {
    val confluenceCredentials = getConfluenceCredentials
    val token: Token = Token.getInstance
    token.initialise(confluenceCredentials.username, confluenceCredentials.password)
    token
  }


}