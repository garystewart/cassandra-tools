package team.supernova.reporting.excel

import team.supernova.reporting.confluence.Overview.KeyspaceInfo
import com.norbitltd.spoiwo.model.{Row, Sheet}
import com.norbitltd.spoiwo.natures.xlsx.Model2XlsxConversions._
import com.norbitltd.spoiwo.model.{Color, Column, Font, CellStyle}
import com.norbitltd.spoiwo.model.enums.CellFill

/**
 * Created by Gary Stewart on 4-8-2015.
 *
 */

object Excel {

  def createSheet (keyspaceInfoList: List[KeyspaceInfo]) = {

    val headerStyle =  CellStyle(fillPattern = CellFill.Solid, fillForegroundColor = Color.AquaMarine, fillBackgroundColor = Color.AquaMarine, font = Font(bold = true))

    val rows = Row(style = headerStyle).withCellValues("Keyspace", "Cluster Name", "linkedCluster", "dcabGroup","dcabApprover","devContacts", "opsContacts" ) +:
      keyspaceInfoList.map(k => Row().withCellValues(
        k.keyspace,
        Option(k.clusterName).getOrElse(""),
        Option(k.linkedCluster).getOrElse(""),
        Option(k.dcabGroup).getOrElse(""),
        Option(k.dcabApprover.foldLeft(""){(a,b) => b + "\n" + a } ).getOrElse(""),
        Option(k.devContacts.foldLeft(""){(a,b) => b + "\n" + a } ).getOrElse(""),
        Option(k.opsContacts.foldLeft(""){(a,b) => b + "\n" + a } ).getOrElse("")
      )).toArray

    val exportSheet = Sheet(name = "KeyspaceInfo")
      .withRows(rows)
      .withColumns(
        Column(index = 0, style = CellStyle(font = Font(bold = true)), autoSized = true)
      )

    println (exportSheet.toString())
    exportSheet.saveAsXlsx("export.xlsx")
  }

}
