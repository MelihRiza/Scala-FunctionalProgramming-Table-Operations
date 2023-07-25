import util.Util.Row
import util.Util.Line
import TestTables.{programmingLanguages1, table1, table3_4_merged, tableFunctional, tableImperative, tableObjectOriented, test3_newCol_Value}

import scala.:+
import scala.annotation.tailrec
import scala.collection.immutable.SeqMap

trait FilterCond {
  def &&(other: FilterCond): FilterCond = {
    (r: Row) => {
      (this.eval(r), other.eval(r)) match {
        case (Some(true), Some(true)) => Some(true)
        case _ => Some(false)
      }
    }
  }

  def ||(other: FilterCond): FilterCond = {
    (r: Row) => {
      (this.eval(r), other.eval(r)) match {
        case (Some(true), _) => Some(true)
        case (_, Some(true)) => Some(true)
        case _ => Some(false)
      }
    }
  }
  // fails if the column name is not present in the row
  def eval(r: Row): Option[Boolean]
}
case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    r.get(colName).map(predicate)
  }
}

case class And(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    if (f2.eval(r).isEmpty || f2.eval(r).isEmpty) None
    else f1.&&(f2).eval(r)
  }
}

case class Or(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    if (f2.eval(r).isEmpty || f2.eval(r).isEmpty) None
    else f1.||(f2).eval(r)
  }
}

trait Query {
  def eval: Option[Table]
}

/*
  Atom query which evaluates to the input table
  Always succeeds
 */
case class Value(t: Table) extends Query {
  override def eval: Option[Table] = Some(t)
}
/*
  Selects certain columns from the result of a target query
  Fails with None if some rows are not present in the resulting table
 */
case class Select(columns: Line, target: Query) extends Query {
  override def eval: Option[Table] = {
    target.eval match {
      case Some(table) => table.select(columns)
      case None => None
    }
  }
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] = {
    target.eval match {
      case Some(table) => table.filter(condition)
      case None => None
    }
  }
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] = {
    target.eval match {
      case Some(table) => Some(table.newCol(name, defaultVal))
      case None => None
    }
  }
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] = {
    val table1 = t1.eval
    val table2 = t2.eval
    (table1, table2) match {
      case (Some(table1), Some(table2)) =>
        if (!table1.getColumnNames.contains(key) || !table2.getColumnNames.contains(key)) {
          None
        } else {
          table1.merge(key, table2)
        }
      case (_, None) => None
      case (None, _) => None
    }
  }
}


class Table (columnNames: Line, tabular: List[List[String]]) {
  def getColumnNames: Line = columnNames

  def getTabular: List[List[String]] = tabular

  // 1.1
  override def toString: String = {
    val columsNames = getColumnNames.mkString(",") + "\n"
    val dim = getTabular.size

    @tailrec
    def construct(i: Int, acc: String): String = {
      if (i == dim) acc + getTabular.drop(i - 1).head.mkString(",")
      else {
        construct(i + 1, acc + getTabular.drop(i - 1).head.mkString(",") + "\n")
      }
    }

    val rez = construct(1, "")

    columsNames + rez
  }

  // 2.1
  def select(columns: Line): Option[Table] = {
    val searchedColumns: List[Int] = columns.map { column =>
      columnNames.indexOf(column) match {
        case -1 => -1
        case idx => idx
      }
    }
    if (searchedColumns.contains(-1)) None
    else {
      val selectedTabular = tabular.map { row =>
        val temp = row.zipWithIndex
        temp.map { temp1 =>
          if (searchedColumns.contains(temp1._2)) temp1._1
          else "TO_REMOVE"
        }
      }
      val temp2 = selectedTabular.map(row => row.filterNot(elem => elem == "TO_REMOVE"))

      Some(new Table(columns, temp2))
    }
  }

  // 2.2
  def filter(cond: FilterCond): Option[Table] = {

    val zippedColNameWithElem = tabular.map {
      row => columnNames.zip(row)
    }

    val zippedToMap = zippedColNameWithElem.map {  // transformare in map, am folosit SeqMap pentru pastrarea ordinii initiale
      list => SeqMap(list: _*)                 // ale elementelor din lista
    }

    val filtered = zippedToMap.filter(ROW => cond.eval(ROW).contains(true))

    val result = filtered.map {    // Revenire la List[List[String]] din map
      ROW => ROW.values.toList
    }

    if (filtered.nonEmpty) {
      Some(new Table(columnNames, result))
    } else {
      None
    }
  }

  // 2.3.
  def newCol(name: String, defaultVal: String): Table = {
    val newColumnNames = columnNames.appended(name)
    val newTabular = tabular.map {
      row => row.appended(defaultVal)
    }
    new Table(newColumnNames, newTabular)
  }

  // 2.4.
  def merge(key: String, other: Table): Option[Table] = {
    val thisKeyIndex = getColumnNames.indexOf(key)
    val otherKeyIndex = other.getColumnNames.indexOf(key)

    if (thisKeyIndex == -1 || otherKeyIndex == -1) {
      None
    } else {

      val mapFirstTable = this.getTabular.map {
        row => this.getColumnNames.zip(row).toMap
      }
      val mapSecondTable = other.getTabular.map {
        row => other.getColumnNames.zip(row).toMap
      }

      val mergedColumnNames = this.getColumnNames ++ other.getColumnNames
      val uniqueColumnNames = mergedColumnNames.distinct

      val temp = mapFirstTable.map {
        row =>
          uniqueColumnNames.map {
            columnName => row.getOrElse(columnName, "")
          }
      }
      val temp2 = mapSecondTable.map {
        row =>
          uniqueColumnNames.map {
            columnName => row.getOrElse(columnName, "")
          }
      }

      def swapColumns(table : List[List[String]], pos: Int): List[List[String]] = {
        table.map {
          row =>
            val tempForSwap = row(pos)
            row.updated(pos, row.head).updated(0, tempForSwap)
        }
      }

      if (thisKeyIndex != 0) {
        swapColumns(temp, thisKeyIndex)
      }
      if (otherKeyIndex != 0) {
        swapColumns(temp2, thisKeyIndex)
      }

      val Map1 = (temp ++ temp2).groupBy(_.head)


      val mergedList = Map1.map {
        entry =>
          if (entry._2.size == 2) entry._2.head.zip(entry._2.tail.head).map {
            case ("", y) => y
            case (x, "") => x
            case (x, y) if x == y => x
            case (x, y) => x + ";" + y
          }
          else entry._2.head
      }.toList

      Some(new Table(uniqueColumnNames, mergedList))
    }
  }

}

object Table {
  // 1.2
  def apply(s: String): Table = {

    val lines = s.split("\n")
    val titles = lines.head.split(",").toList
    val rez = lines.drop(1).map(_.split(",", -1).toList).toList
    new Table(titles, rez)
  }
}
