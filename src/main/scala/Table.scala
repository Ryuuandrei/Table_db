import util.Util.Row
import util.Util.Line
import TestTables.tableImperative
import TestTables.tableFunctional
import TestTables.tableObjectOriented

trait FilterCond {
  def &&(other: FilterCond): FilterCond = And(this, other)

  def ||(other: FilterCond): FilterCond = Or(this, other)

  // fails if the column name is not present in the row
  def eval(r: Row): Option[Boolean]
}

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] =
    if (!r.contains(colName)) None
    else Some(predicate(r(colName)))
}

case class And(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] =
    if (f1.eval(r).isEmpty || f2.eval(r).isEmpty) None
    else Some(f1.eval(r).get && f2.eval(r).get)
}

case class Or(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] =
    if (f1.eval(r).isEmpty || f2.eval(r).isEmpty) None
    else Some(f1.eval(r).get || f2.eval(r).get)
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
  override def eval: Option[Table] = target.eval.get.select(columns)
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] = target.eval.get.filter(condition)
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] = Some(target.eval.get.newCol(name, defaultVal))
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] = t1.eval.get.merge(key, t2.eval.get)
}


class Table(columnNames: Line, tabular: List[List[String]]) {
  def getColumnNames: Line = columnNames

  def getTabular: List[List[String]] = tabular

  // 1.1
  override def toString: String = columnNames.reduce(_ + ',' + _) + '\n' + tabular.map(_.reduce(_ + ',' + _)).reduce(_ + '\n' + _)

  // 2.1
  def select(columns: Line): Option[Table] = {
    val col = columnNames.zipWithIndex.filter(x => columns.contains(x._1)).map(_._2)
    if (col.isEmpty) return None

    val newTab = for (line <- tabular)
      yield for (i <- col)
        yield line(i)
    Some(new Table(columns, newTab))
  }

  // 2.2
  def filter(cond: FilterCond): Option[Table] = {

    if (cond.eval(columnNames.zip(tabular.head).toMap).isEmpty) return None

    val newTab = for (line <- tabular)
      yield cond.eval(columnNames.zip(line).toMap) match {
        case Some(pred) => if (pred) line else Nil
      }
    Some(new Table(columnNames, newTab.filter(_ != Nil)))
  }


  // 2.3.
  def newCol(name: String, defaultVal: String): Table = {
    new Table(columnNames ::: List(name), tabular.map(_ ::: List(defaultVal)))
  }

  // 2.4.
  def merge(key: String, other: Table): Option[Table] = {

    if (!columnNames.contains(key) || !other.getColumnNames.contains(key)) None
    else {
      val newColumns = (columnNames ::: other.getColumnNames).distinct
      val index = columnNames.indexOf(key)
      val tab1 = tabular.transpose.drop(0)(index)
      val tab2 = other.getTabular.transpose.drop(0)(index)
      val allKeys = (tab1 ::: tab2).distinct

      val a = tab1.zip(tabular.map(_.filter(!tab1.contains(_)))).map(x => (x._1, columnNames.filter(_ != key).zip(x._2).toMap)).toMap
      val b = tab2.zip(other.getTabular.map(_.filter(!tab2.contains(_)))).map(x => (x._1, other.getColumnNames.filter(_ != key).zip(x._2).toMap)).toMap

      val mergedTable = for (k <- allKeys)
        yield for (c <- newColumns)
          yield
            if (c == key) k
            else if (a.contains(k) && b.contains(k)) {

              if (a(k).contains(c) && b(k).contains(c)) {
                if (a(k)(c) != b(k)(c)) a(k)(c) + ';' + b(k)(c)
                else a(k)(c)
              }
              else if (a(k).contains(c)) a(k)(c)
              else b(k)(c)

            } else {

              if (a.contains(k)) {
                if (a(k).contains(c)) a(k)(c)
                else ""
              } else {
                if (b(k).contains(c)) b(k)(c)
                else ""
              }

            }
      Some(new Table(newColumns, mergedTable))
    }
  }
}

object Table {
  // 1.2
  def apply(s: String): Table = {
    val str = s.split('\n').toList
    new Table(str.take(1).flatMap(x => x.split(',')), str.drop(1).map(x => x.split(",", -1).toList))
  }
}
