package cn.zwl.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount1 {
  def main(args: Array[String]): Unit = {
    val params=ParameterTool.fromArgs(args)
    val host:String=params.get("host")
    val port:Int=params.getInt("port")

    //1,创建流处理的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    //接受socket数据流
    val textDataStream=env.socketTextStream(host,port)

    //逐一读取数据，打散进行wordcount

    val WordCountDataStream=textDataStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      .map( (_,1) )
      .keyBy(0)
      .sum(1)

    WordCountDataStream.print().setParallelism(2)


    //执行任务
    env.execute("Stream word Count Job")

  }
}
