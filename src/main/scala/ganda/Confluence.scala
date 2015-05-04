package ganda

import nl.ing.confluence.rpc.soap.actions.Page
import nl.ing.confluence.rpc.soap.beans.RemotePage

import scala.xml.NodeSeq

object Confluence {

  def confluenceCreatePage (project: String, pageName: String, content: String, pageObject: Page, parentPage: RemotePage) : Unit = {
    val contentMD5 = "MD5:" + MD5.hash(content)
    val finalContent = content + "<br/>" + contentMD5
    //println(finalContent)
    try {
      //TODO cluster names is required in name!
      val existingPage: RemotePage = pageObject.read(project,pageName)
      if (!existingPage.getContent.toString.contains(contentMD5) ) {
        println (s"$pageName page updated.")
        existingPage.setContent( finalContent)
        pageObject.store(existingPage)
      } else {
        println (s"$pageName page not updated!")
      }
    }
    catch {
      case e: Exception => {
        val newPage: RemotePage = new RemotePage
        newPage.setContent(finalContent)
        newPage.setTitle( pageName)
        newPage.setParentId(parentPage.getId)
        newPage.setSpace(parentPage.getSpace)
        pageObject.store(newPage)
        println (s"$pageName created!")
      }
    }
  }

  //https://confluence.atlassian.com/display/DOC/Code+Block+Macro
  def confluenceCodeBlock (title: String, data: String, lang: String): NodeSeq={
    if (!data.trim.isEmpty) {
      <ac:structured-macro ac:name="code">
        <ac:parameter ac:name="title">{title}</ac:parameter>
        <ac:parameter ac:name="theme">Default</ac:parameter>
        <ac:parameter ac:name="linenumbers">false</ac:parameter>
        <ac:parameter ac:name="language">{lang}</ac:parameter>
        <ac:parameter ac:name="firstline"></ac:parameter>
        <ac:parameter ac:name="collapse">true</ac:parameter>
        <ac:plain-text-body>{ scala.xml.Unparsed("<![CDATA[%s]]>".format(data))}</ac:plain-text-body>
      </ac:structured-macro>
    } else {NodeSeq.Empty}
  }
}