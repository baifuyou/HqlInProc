package com.scratbai.hqlinproc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf

/**
  * @author baifuyou 2018/8/7
  */
object ScalaApp {
  type ExpressionCalculator = Row => Any
  type ExpressionPredict = Row => Boolean
  type MultiExpressionCalculator = Row => Seq[Any]
  type AggColCalculator = Seq[Row] => Col
  type AggMultiColCalculator = Seq[Row] => Seq[Col]
  type SqlExecutor = Context => Table

  def main(args: Array[String]): Unit = {
    test1()
  }

  def test2(): Unit = {
    val sqlConf = new SQLConf
    val sparkSqlParser = new SparkSqlParser(sqlConf)
    val logicalPlan = sparkSqlParser.parsePlan("select abxxxxs(b) from test_table where a = 44")

    parse(logicalPlan)
    System.out.println(logicalPlan)
  }

  def test1(): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      //      .master("yarn")
      .master("local")
      //      .enableHiveSupport()
      .getOrCreate()
    val descs = spark.createDataFrame(Seq(("代发款项信用卡还款", 24.0), ("POS消费银联AP摩拜单车租车云闪付", 37.8)
      , ("POS消费银联快尚时装滨江天街店ACC", 23.9), ("消费京东支付", 85.7), ("余额分期当月扣款余额转分期十二期</>", 48.6)
      , ("分期手续费", 59.9))).toDF("description", "cnt")
    descs.createTempView("tmp_descs")
    val df2 = spark.sql("select abs(cnt) as abs_cnt from tmp_descs where cnt > 50")
    val logicPlan = df2.queryExecution.analyzed

    println("---------------")
    val start = System.currentTimeMillis()
    df2.show(10)
    val end = System.currentTimeMillis()
    println(s"cost: ${end - start}")
  }

  def aggregate(groupingExpressions: Seq[Expression], aggregateExpressions: Seq[NamedExpression], child: LogicalPlan): SqlExecutor = {
    val multiExpressionCalculator = expressions(groupingExpressions)
    val multiColCalculator = namedExpressionsOnMultiRow(aggregateExpressions)
    val sqlExecutor = parse(child)
    context => {
      val table = sqlExecutor(context)
      table.groupby(multiExpressionCalculator, multiColCalculator)
    }
  }

  def namedExpressionsOnMultiRow(expressions: Seq[NamedExpression]): AggMultiColCalculator = {
    val colCalculator = expressions.map(e => namedExpressionOnMultiRow(e))
    row => {
      colCalculator.map(e => e(row))
    }
  }

  def namedExpressionOnMultiRow(expression: NamedExpression): AggColCalculator = {
    _ => Col("", 0.0)
  }

  def expressions(expressions: Seq[Expression]): MultiExpressionCalculator = {
    val expressionCalculators = expressions.map(e => expression(e))
    row => {
      expressionCalculators.map(e => e(row))
    }
  }

  def expression(expression: Expression): ExpressionCalculator = {
    val code = expression.genCode(new CodegenContext())
    println(code)
    _ => 0.0
  }

  def evalExpressionGetBoolean(expression: Expression): ExpressionPredict = {
    val expressionCalculator = this.expression(expression)
    row => {
      val resultAny = expressionCalculator(row)
      if (resultAny.isInstanceOf[Boolean])
        resultAny.asInstanceOf[Boolean]
      else
        throw new IllegalArgumentException("where 条件的结果必须是 Boolean 类型")
    }
  }

  def filter(condition: Expression, child: LogicalPlan): SqlExecutor = {
    val expressionPredict = evalExpressionGetBoolean(condition)
    val sqlExecutor = parse(child)
    context => {
      val table = sqlExecutor(context)
      table.filter(expressionPredict)
    }
  }

  def unresolvedRelation(tableIdentifier: TableIdentifier, alias: Option[String]): SqlExecutor = {
    val tableName = tableIdentifier.unquotedString
    context => {
      context.getTable(tableName)
    }
  }

  def project(projectList: Seq[NamedExpression], child: LogicalPlan): SqlExecutor = {
    for (e <- projectList) {
      val code = e.genCode(new CodegenContext)
      println(code)
    }
    _ => null
  }

  def parse(logicalPlan: LogicalPlan): SqlExecutor = {
    logicalPlan match {
      case Aggregate(groupingExpressions, aggregateExpressions, child) => aggregate(groupingExpressions, aggregateExpressions, child)
      case Filter(condition, child) => filter(condition, child)
      case UnresolvedRelation(tableIdentifier, alias) => unresolvedRelation(tableIdentifier, alias)
      case Project(projectList, child) => project(projectList, child)
    }
  }
}
