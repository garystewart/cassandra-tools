package team.supernova

import nl.ing.confluence.rpc.soap.actions.Page
import nl.ing.confluence.rpc.soap.beans.RemotePage

import scala.xml.NodeSeq

object Confluence {

  //update a page if the token is unknown or create a confluence page if the page does not exist
  def confluenceCreatePage(project: String, pageName: String, content: String, pageObject: Page, parentPage: RemotePage, tokenPage: RemotePage): String = {

    val contentMD5 = pageName + "_MD5:" + MD5.hash(content)

    //if the contentMD5 exists in TOKEN no need to read and update confluence!

      try {
        println(s"Checking ${tokenPage.getTitle} for page: $pageName ")
        if (!tokenPage.getContent.toString.contains(contentMD5)) {
          println(s"Updating: $pageName")
          val existingPage: RemotePage = pageObject.read(project, pageName)
          existingPage.setContent(content)
          pageObject.store(existingPage)
          println(s"$pageName page updated.")
        } else {
          println(s"$pageName page not updated!")
        }
      }
      catch {
        case e: Exception => {
          val newPage: RemotePage = new RemotePage
          newPage.setContent(content)
          newPage.setTitle(pageName)
          newPage.setParentId(parentPage.getId)
          newPage.setSpace(parentPage.getSpace)
          pageObject.store(newPage)
          println(s"$pageName created!")
        }
      }


    //return the token
    contentMD5
  }

  //https://confluence.atlassian.com/display/DOC/Code+Block+Macro
  //DONT AUTO-FORMAT THIS OTHERWISE IT WILL FAIL ON CONFLUENCE!!!
  def confluenceCodeBlock(title: String, data: String, lang: String): NodeSeq = {
    if (!data.trim.isEmpty) {
      <ac:structured-macro ac:name="code">
        <ac:parameter ac:name="title">{title}</ac:parameter>
        <ac:parameter ac:name="theme">Default</ac:parameter>
        <ac:parameter ac:name="linenumbers">false</ac:parameter>
        <ac:parameter ac:name="language">{lang}</ac:parameter>
        <ac:parameter ac:name="firstline"></ac:parameter>
        <ac:parameter ac:name="collapse">true</ac:parameter>
        <ac:plain-text-body>{scala.xml.Unparsed("<![CDATA[%s]]>".format(data))}</ac:plain-text-body>
      </ac:structured-macro>
    } else {
      NodeSeq.Empty
    }
  }
}