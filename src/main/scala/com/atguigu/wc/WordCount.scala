package com.atguigu.wc

import org.apache.flink.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.wc
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/16 11:48
  */

// 批处理代码
object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPath = "E:\\桌面\\文件夹\\Final\\技术\\大数据技术资料\\生态框架学习资料\\Flink\\笔记资料代码\\3.代码\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDataSet = env.readTextFile(inputPath)

    // 分词之后做count
    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map( (_, 1) )
      .groupBy(0)
      .sum(1)

    // 打印输出
    wordCountDataSet.print()
  }
}
