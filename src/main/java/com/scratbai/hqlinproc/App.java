package com.scratbai.hqlinproc;

import com.scratbai.hqlinproc.ast.HplsqlLexer;
import com.scratbai.hqlinproc.ast.HplsqlParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SparkSqlParser;
import org.apache.spark.sql.internal.SQLConf;

/**
 * @author baifuyou 2018/8/6
 */
public class App {
    public static void main(String[] args) {
        sparkSqlParse();
    }

    public static void sparkSqlParse() {
        SparkSqlParser sparkSqlParser = new SparkSqlParser(new SQLConf());
        LogicalPlan logicalPlan = sparkSqlParser.parsePlan("select b, sun(c) as d from test_table where a = 44 group by b");
        System.out.println(logicalPlan);
    }

    public static void antlrParse() {
        ANTLRInputStream antlrInputStream = new ANTLRInputStream("select b, sun(c) as d from test_table where a = 44 group by b");
        HplsqlLexer hplsqlLexer = new HplsqlLexer(antlrInputStream);
        CommonTokenStream commonTokenStream = new CommonTokenStream(hplsqlLexer);
        HplsqlParser hplsqlParser = new HplsqlParser(commonTokenStream);
        HplsqlParser.BlockContext block = hplsqlParser.block();
        System.out.println(block);
    }
}
