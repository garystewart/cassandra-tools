package team.supernova.reporting.confluence

import java.util.Properties
import team.supernova.confluence.soap.rpc.soap.actions.Token


/**
 * Created by Gary Stewart on 4-8-2015.
 *
 */

object ConfluenceToken {

  def getConfluenceToken (propFileName: String): Token = {
    val props: Properties = new Properties
    props.load(this.getClass.getClassLoader.getResourceAsStream(propFileName))
    val confluenceUser = props.getProperty("confluence.user")
    val confluencePassword = props.getProperty("confluence.password")
    val endpointURL = props.getProperty("confluence.endpointaddress")
    val token: Token = Token.getInstance

    token.initialise(confluenceUser, confluencePassword,endpointURL )
    token
  }

}