package com.scratbai.hqlinproc

/**
  * @author baifuyou 2018/8/7
  */
class MemTable(var alias: Option[String] = None) extends Table {
  override def filter(predict: Row => Boolean): Table = ???

  override def groupby(by: Row => Seq[Any], aggregate: Seq[Row] => Seq[Col]): Table = ???
}
