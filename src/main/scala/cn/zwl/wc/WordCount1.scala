package cn.zwl.wc

import org.apache.flink.api.scala._

object WordCount1 {
  def main(args: Array[String]): Unit = {
    val env=ExecutionEnvironment.getExecutionEnvironment


    //从文件中读取数据
    val inpath="E:\\桌面\\文件夹\\Final\\技术\\大数据技术资料\\生态框架学习资料\\Flink\\笔记资料代码\\3.代码\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inpath)

    val wordSet: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    wordSet.print()

  }
}
