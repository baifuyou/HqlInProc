package com.scratbai.hqlinproc

/**
  * @author baifuyou 2018/8/7
  */
trait Table {
  def filter(predict: Row => Boolean): Table
  def groupby(by: Row => Seq[Any], aggregate: Seq[Row] => Seq[Col]): Table
}

object Table {
  def fromListMap(list: List[Map[String, Any]]): Table = {
    return null
  }
}
