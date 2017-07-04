// Copyright (C) 2017 EDMA team & other authors
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.edma.hbase

import play.api.libs.json._
import play.api.libs.functional.syntax._

case class SchemaTable(id: String, desc: String, rowkey: SchemaRowKey, colFamilies: Seq[SchemaColumnFamily]) {
  def toPrintable: String = {
    val table: String = f"""
    |Table id : $desc
    |  Key ${rowkey.toPrintable}
    |""".stripMargin
    
    colFamilies.foldLeft(table)((a, cf) => a + cf.toPrintable)
  }
}

case class SchemaRowKey(desc: String, syntax: String, sample: String) {
  def toPrintable: String = f"$desc (eg: $sample)"
}

case class SchemaColumnFamily(name: String, desc: String, colQualifiers: Seq[SchemaColumnQualifier]) {
  def toPrintable: String = {
    val cf: String = f"""
    |  Cf $name : $desc
    |""".stripMargin
    
    colQualifiers.foldLeft(cf)((a, cq) => a + cq.toPrintable)
  }
}

case class SchemaColumnQualifier(name: String, ctype: String, syntax: Option[String], searchable: Option[Boolean], desc: String, sample: Option[String], flag: Option[String], ref: Option[String]) {
  def toPrintable: String = {
    val cq: String = f"""
    |    Cq $name: $ctype = $desc (eg: $sample)
    |""".stripMargin
    
    cq
  }
}

class HSchema(table: HTable) {
  
  /** empty schema */
  private val noKey: SchemaRowKey = SchemaRowKey("???", "Unknown row key", "")
  val noSchema: SchemaTable = SchemaTable(table.tname, "Schema not found", noKey, Seq.empty[SchemaColumnFamily])
  
  /** schema as found either from table or from data */
  val schema: SchemaTable = {
    val declared = loadFromTable
    if (declared == noSchema) {
      inferFromTable
    } else {
      declared
    }
  }

  private def loadFromTable: SchemaTable = {
    val rawSchema: Array[Byte] = table.get(schemaRowId, schemaCfId, schemaCqId)
    if (rawSchema.length > 0) {
      try { 
        jsonToSchema(rawSchema) 
      } catch {
        case any: Throwable => {
          error(any.getMessage)
          noSchema
        }
      }
    } else {
      error("Schema not found")
      noSchema
    }
  }
  
  /** infer schema from table
   *  Not yet implemented
   */
  private def inferFromTable: SchemaTable = noSchema
  // TODO: Implement inferFromTable
  // Idea: Parse n rows ; discover column families, column qualifiers, data types (simple heuristics)
  
  /** convert json schema as SchemaTable */
  private def jsonToSchema(rawJSon: String): SchemaTable = {
    val json: JsValue = Json.parse(rawJSon)
    
    implicit val schemaColumnQualifierReads: Reads[SchemaColumnQualifier] = (
      (JsPath \ "name").read[String] and
      (JsPath \ "type").read[String] and
      (JsPath \ "syntax").readNullable[String] and
      (JsPath \ "searchable").readNullable[Boolean] and
      (JsPath \ "description").read[String] and
      (JsPath \ "sample").readNullable[String] and
      (JsPath \ "flag").readNullable[String] and
      (JsPath \ "ref").readNullable[String]
    )(SchemaColumnQualifier.apply _)
    
    implicit val schemaColumnFamilyReads: Reads[SchemaColumnFamily] = (
      (JsPath \ "name").read[String] and
      (JsPath \ "description").read[String] and
      (JsPath \ "columnQualifiers").read[Seq[SchemaColumnQualifier]]
    )(SchemaColumnFamily.apply _)

    implicit val schemaRowReads: Reads[SchemaRowKey] = (
      (JsPath \ "description").read[String] and
      (JsPath \ "syntax").read[String] and
      (JsPath \ "example").read[String]
    )(SchemaRowKey.apply _)

    implicit val schemaTableReads: Reads[SchemaTable] = (
      (JsPath \ "id").read[String] and
      (JsPath \ "description").read[String] and
      (JsPath \ "rowkey").read[SchemaRowKey] and
      (JsPath \ "columnFamilies").read[Seq[SchemaColumnFamily]]
    )(SchemaTable.apply _)

    val schemaResult: JsResult[SchemaTable] = json.validate[SchemaTable]
    if (schemaResult.isSuccess) {
      schemaResult.get
    } else {
      echo("Unable to read schema from JSon string for table " + table.tname)
      echo(rawJSon)
      noSchema
    }
  }
  
  def toJSon: String = {
    
    implicit val schemaColumnQualifierWrites = new Writes[SchemaColumnQualifier] {
      def writes(cq: SchemaColumnQualifier) = Json.obj(
        "name" -> cq.name,
        "type" -> cq.ctype,
        "syntax" -> cq.syntax,
        "searchable" -> cq.searchable,
        "description" -> cq.desc,
        "sample" -> cq.sample,
        "flag" -> cq.flag,
        "ref" -> cq.ref
      )
    }
    
    implicit val schemaColumnFamilyWrites = new Writes[SchemaColumnFamily] {
      def writes(cf: SchemaColumnFamily) = Json.obj(
        "name" -> cf.name,
        "description" -> cf.desc,
        "columnQualifiers" -> cf.colQualifiers
      )
    }

    implicit val schemaRowWrites = new Writes[SchemaRowKey] {
      def writes(rk: SchemaRowKey) = Json.obj(
        "description" -> rk.desc,
        "syntax" -> rk.syntax,
        "sample" -> rk.sample
      )
    }

    implicit val schemaTableWrites = new Writes[SchemaTable] {
      def writes(t: SchemaTable) = Json.obj(
        "id" -> t.id,
        "description" -> t.desc,
        "rowkey" -> t.rowkey,
        "columnFamilies" -> t.colFamilies
      )
    }

    Json.toJson(schema).toString
  }
  
  def toPrintable: String = schema.toPrintable

}

object HSchema {

  def schemaSyntax: String = """
{
  "id": "Nom_de_la_table",
  "description": "Ceci est la table HBASE de l'objet Truc",
  "rowkey": {
      "description": "Clé primaire de l'objet truc, au format XX-NN",
      "syntax": "Expression régulière sur la syntaxe attendue – format à choisir eg Perl", // Permettra des contrôles de clé
      "example": "AB-12"
    },
  "columnFamilies": [
    { "name": "d",
      "description": "CF principale des données",
      "columnQualifiers": [
          {
            "name": "colonne1",
            "type": "string",
            "syntax": "Expression régulière – format à choisir", // Optionnel, permettra des contrôles de valeur
            "searchable": true,                                  // définit si la valeur est indexée sous ES
            "description": "Nom du client, ...",
            "example": "DUPOND"
          },
          {
            "name": "colonne2",
            "type": "string",
            "description": "Prénom du client, ...",
            "example": "Jean"
         },
          // and so on
        ]
    },
    { "name": "l",
      "description": "CF de liaisons",
      "columnQualifiers": [
        {
          "name": "f_{id}",
          "flag": "dynamic",     // optionnel, « static » par défaut
          "ref": "moe_Facture",  // Table de référence pour la liaison (pour laquelle {id} est définie comme rowkey)
          "type": "json",
          "description": "Synthèse de facture liée",
          "example": "{ \"id\": \"123456\", \"MontantTTC\": 123.45 }"
        },
        // and so on
      ]
    },
   // and so on
  ]
}
"""

   def sampleSchema: String = """{"id":"dco_edma:Object","description":"Table test EDMA","rowkey":{"description":"Clé primaire des objets, au format NNNNNN","syntax":"[0-9]+","example":"000000"},"columnFamilies":[{"name":"d","description":"CF principale des données","columnQualifiers":[{"name":"col1","type":"String","syntax":"Texte libre","searchable":true,"description":"Colonne de type texte","example":"Hello, world!"},{"name":"col2","type":"Int","description":"Colonne de type entier","example":"123"},{"name":"col3","type":"Boolean","description":"Colonne de type booléen","example":"true"}]}]}"""
}