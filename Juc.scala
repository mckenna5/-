package shixun4

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, format_number, sum, when}
import org.apache.spark.sql.types.DataTypes

object Juc {

  val BaseUrl = "hdfs://192.168.49.128:8020"

  def main(args: Array[String]): Unit = {
//    read()
//    fun1()
//    fun2()
//    fun3()
//    fun4()
//    fun5()
//    fun6()
//    fun7()
//    fun8()
//    fun9()
  }


  private def createSpark(appName: String, master: String): SparkSession = {

    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val spark = SparkSession.builder()
      //本地运行配置
      .master(master)
      .appName(appName)
      //解决spark日期格式转换配置
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

    return spark;
  }

  protected def read(): Unit = {

    // 创建spark的session，并设置master为local[2]（可以根据自己的需求设置）
    val spark = createSpark("Func1comm_spark_hdfs", "local[2]")

    // 读取HDFS上的数据集
    val df = spark.read.option("header", true).csv(BaseUrl + "/data/juc1-clear.csv")

    // 打印数据集的结构和样本数据
    df.printSchema() // 显示数据结构

    df.show() // 显示数据的前20行
  }

  /*
  功能需求1:
  实现数据分析，计算还款天数差异计算，还款天数是新产生的数据列
  要求，根据原始数据集，产生处理后的新数据集，重点体现还款天数的差异，以借后续BI进行数据可视化。
  公式： <到期日期>  减去 <还款日期> = <还款天数差异>
   */
  protected def fun1(): Unit = {

    val spark = createSpark("F1DataFilter", "local[2]")

    // 读取HDFS上的数据集
    val df = spark.read.option("header", true).csv(BaseUrl + "/data/juc1-clear.csv")

    // 处理数据
    val resDF = df.withColumn("到期日期", functions.to_date(df.col("到期日期"), "yyyy/M/d"))
      .withColumn("还款日期", functions.to_date(df.col("还款日期"), "yyyy/M/d"))
      .withColumn("还款天数差异", functions.datediff(col("到期日期"), col("还款日期")))

    // 保存数据
    resDF.write.format("csv").option("header", true).save(BaseUrl + "/JucRes/1.还款天数差异")

    //释放sparkSession对象
    spark.close()

  }

  /*
  功能需求2：
  从性别和年份特征进行分析借款金额
   */
  protected def fun2(): Unit = {

    val spark = createSpark("F2DataFilter", "local[2]")

    // 读取HDFS上的数据集
    val df = spark.read.option("header", true).csv(BaseUrl + "/data/juc2-clear.csv")

    //处理数据
    val resDF = df.withColumn("借款成功日期", functions.to_date(df.col("借款成功日期"), "yyyy"))
      .withColumn("借款金额", col("借款金额").cast(DataTypes.DoubleType))
      .groupBy("借款成功日期", "性别")
      .sum("借款金额")

    // 保存数据
    resDF.write.format("csv").option("header", true).save(BaseUrl + "/JucRes/2.按性别和年份分析借款金额")

    //释放sparkSession对象
    spark.close()

  }

  /*
  功能需求3
  按年份和新老客户分析借款金额
  对数据集进行新老客户分析，计算新客户和老客户的借款金额总和.
   */
  protected def fun3(): Unit = {

    val spark = createSpark("F3DataFilter", "local[2]")

    // 读取 HDFS 上的数据集
    val df = spark.read.option("header", true).csv(BaseUrl + "/data/juc2-clear.csv")

    // 处理数据
    val resDF = df
      .withColumn("借款成功日期", functions.to_date(col("借款成功日期"), "yyyy")) // 提取年份
      .withColumn("借款金额", col("借款金额").cast(DataTypes.DoubleType)) // 转换借款金额为 Double 类型
      .withColumn("新老客户", when(col("是否首标") === "是", "新客户").otherwise("老客户")) // 将是否首标改为新老客户
      .groupBy("借款成功日期", "新老客户") // 按年份和新老客户分组
      .sum("借款金额") // 计算借款金额总和

    // 保存数据
    resDF.write
      .format("csv")
      .option("header", true)
      .save(BaseUrl + "/JucRes/3.按年份和新老客户分析借款金额")

    // 释放 SparkSession 对象
    spark.close()
  }

  /*
  功能需求4
  分析学历和年份与借款金额的关系
  对数据集中的学历进行分析，计算学历与借款金额之间的关系，
  对学历的认证情况进行筛选，分为学历认证与未成功认证。
   */
  protected def fun4(): Unit = {

    val spark = createSpark("F4DataFilter", "local[2]")

    // 读取 HDFS 上的数据集
    val df = spark.read.option("header", true).csv(BaseUrl + "/data/juc2-clear.csv")

    //处理数据
    val resDF = df.withColumn("借款成功日期", functions.to_date(col("借款成功日期"), "yyyy")) // 提取年份
      .withColumn("借款金额", col("借款金额").cast(DataTypes.DoubleType))
      .groupBy("借款成功日期", "学历认证") // 按年份和新老客户分组
      .sum("借款金额") // 计算借款金额总和

    // 保存数据
    resDF.write
      .format("csv")
      .option("header", true)
      .save(BaseUrl + "/JucRes/4.分析学历和年份与借款金额的关系")

    //释放sparkSession对象
    spark.close()

  }

  /*
  功能需求5：
  分析年龄段与借款金额的关系
  对数据集进行借款年龄分析，
  计算不同年龄段的借款金额总和，以此来判断，什么年龄段是借款活跃区间。
   */
  protected def fun5(): Unit = {

    val spark = createSpark("F5DataFilter", "local[2]")

    // 读取 HDFS 上的数据集
    val df = spark.read.option("header", true).csv(BaseUrl + "/data/juc2-clear.csv")

    // 数据处理
    val resDF = df.withColumn("年龄", col("年龄").cast(DataTypes.IntegerType))
      .withColumn("借款金额", col("借款金额").cast(DataTypes.DoubleType))

    // 提取年龄列
    val ageCol = col("年龄")
    //按年龄分组
    val ageGroup = when(ageCol.between(15, 20), "15-20")
      .when(ageCol.between(20, 25), "20-25")
      .when(ageCol.between(25, 30), "25-30")
      .when(ageCol.between(30, 35), "30-35")
      .when(ageCol.between(35, 40), "35-40")
      .otherwise("40+")

    //计算借款金额总和
    val ageDF = resDF.withColumn("ageIdx", ageGroup)
      .groupBy("ageIdx")
      .agg(sum("借款金额").as("总借款金额"))
      .orderBy("ageIdx")
      .show()

    //    // 保存数据
    //    ageDF.write
    //      .format("csv")
    //      .option("header", true)
    //      .save(BaseUrl + "/JucRes/5.分析年龄段与借款金额的关系")

    //释放
    spark.close()
  }

  /*
  功能需求6：
  贷款金额的走势分析
  分析每日贷款金额的走势
  对贷款金额进行趋势分析，计算每日贷款金额的总和。
  1，从原始数据集中选择，<借款成功日期>，<借款金额>列，创建一个新的数据集，只包含这两列。
  2，用withColumn()新增加列，将<借款成功日期>转换为日期类型，新列叫借款日期，就是<借款成功日期>转化来的。
  3，用groupBy("借款日期")进行分组，通过日期聚合函数 agg()计算每日贷款金额的总和，得出趋势，产生新的数据集。
   */
  protected def fun6(): Unit = {

    val spark = createSpark("F6DataFilter", "local[2]")

    // 读取 HDFS 上的数据集
    val df = spark.read.option("header", true).csv(BaseUrl + "/data/juc2-clear.csv")

    // 1.选择需要的列
    val selectedDF = df.select("借款成功日期", "借款金额")

    // 2.类型转换并增加列
    val processedDF = selectedDF.withColumn("借款日期", functions.to_date(col("借款成功日期"), "yyyy/MM/dd"))
      .withColumn("借款金额", col("借款金额").cast(DataTypes.DoubleType))

    // 3. 按日期分组，计算每日贷款金额总和
    val dailyDF = processedDF.groupBy("借款日期")
      .agg(sum("借款金额").as("每日贷款金额总和"))
      .orderBy("借款日期")

    // 保存
    dailyDF.write
      .format("csv")
      .option("header", true)
      .save(BaseUrl + "/JucRes/6.每日贷款金额的走势")

    //释放
    spark.close()
  }

  /*
  功能需求7：
  分析每月贷款金额的走势
   */
  protected def fun7(): Unit = {

    val spark = createSpark("F7DataFilter", "local[2]")

    // 读取 HDFS 上的数据集
    val df = spark.read.option("header", true).csv(BaseUrl + "/data/juc2-clear.csv")

    val selectedDF = df.select("借款成功日期", "借款金额")

    // 2. 转换数据类型并添加新列
    val processedDF = selectedDF
      .withColumn("借款日期", functions.to_date(col("借款成功日期"), "yyyy/MM/dd")) // 转换为日期类型
      .withColumn("借款金额", col("借款金额").cast(DataTypes.DoubleType)) // 转换为 Double 类型
      .withColumn("借款月份", functions.date_format(col("借款日期"), "yyyy/MM")) // 提取年月

    // 3. 按月份分组，计算每月贷款金额总和
    val monthlyDF = processedDF
      .groupBy("借款月份") // 按月份分组
      .agg(sum("借款金额").as("每月贷款金额总和")) // 计算每月贷款金额总和
      .orderBy("借款月份") // 按月份排序

    // 保存
    monthlyDF.write
      .format("csv")
      .option("header", true)
      .save(BaseUrl + "/JucRes/7.每月贷款金额的走势")

    //释放
    spark.close()

  }

  /*
  功能需求8：
  不同借款金额的逾期还款率分析
  和信用有关，主要对数据集进行不同条件的数据划分和分析。
  首先定义三个指标，初始评级level_idx, 借款类型kind_idx, 借款金额amount_idx.
  对于初始评级的划分，使用循环遍历每个评级的指标，用filter和equalTo.选出符合当前评级指标的数据，创建相应的数据集。
  对于借款类型的划分，与初始评级的指标划分一样做法。
  对于不同借款金额的数据划分，用数组amount存储不同金额的范围，以便选出符合不同金额范围的数据。
  对于逾期还款率的分析，使用withColumn增加新列，逾期还款率，使用：历史逾期还款期数 除以 （历史逾期还款期数 加上 历史正常还款期数）并乘以100 得到<逾期还款率>.
  最后，选择需要保留的列，有：<借款类型，借款金额，逾期还款率> 并产生新的数据集。
   */
  protected def fun8(): Unit = {

    val spark = createSpark("F7DataFilter", "local[2]")

    // 读取 HDFS 上的数据集
    val df = spark.read.option("header", true).csv(BaseUrl + "/data/juc2-clear.csv")

    // 初始评级的数据划分
    val level_idx = Array("1", "2", "3", "4", "5", "6")
    val lev = new Array[Object](level_idx.length)
    for (i <- level_idx.indices) {
      lev(i) = df.filter(col("初始评级").equalTo(level_idx(i)))
    }

    //借款类型的数据划分
    val kind_idx = Array("电商", "APP闪电", "普通", "其他")
    val kind = new Array[Object](kind_idx.length)
    for (i <- kind_idx.indices) {
      kind(i) = df.filter(col("借款类型").equalTo(kind_idx(i)))
    }

    // 定义借款金额的分组标签
    val amount_idx = Array("0-2000", "2000-3000", "3000-4000", "4000-5000", "5000-6000", "6000+")
    // 使用 when 表达式对借款金额进行分组
    val amountGroup = when(col("借款金额").between(0, 2000), amount_idx(0))
      .when(col("借款金额").between(2000, 3000), amount_idx(1))
      .when(col("借款金额").between(3000, 4000), amount_idx(2))
      .when(col("借款金额").between(4000, 5000), amount_idx(3))
      .when(col("借款金额").between(5000, 6000), amount_idx(4))
      .otherwise(amount_idx(5)) // 6000+

    //逾期还款率的分析
    var dfWithDepayRate = df.withColumn("逾期还款率", col("历史逾期还款期数")
      .divide(col("历史逾期还款期数").plus(col("历史正常还款期数")))
      .multiply(100))
    // 将逾期还款率保留三位小数
    dfWithDepayRate = dfWithDepayRate.withColumn("逾期还款率", format_number(col("逾期还款率"), 3)).na.drop
    //产生新的数据集
    val resDF = dfWithDepayRate.select(col("借款类型"), col("借款金额"), col("逾期还款率"))

    // 保存
    resDF.write
      .format("csv")
      .option("header", true)
      .save(BaseUrl + "/JucRes/8.不同借款金额的贷款逾期率分析")
    //释放
    spark.close()

  }

  /*
  功能需求9：
  按借款类型分析逾期还款率
   */
  protected def fun9(): Unit = {
    val spark = createSpark("F7DataFilter", "local[2]")

    // 读取 HDFS 上的数据集
    val df = spark.read.option("header", true).csv(BaseUrl + "/data/juc2-clear.csv")

    // 初始评级的数据划分
    val level_idx = Array("1", "2", "3", "4", "5", "6")
    val lev = new Array[Object](level_idx.length)
    for (i <- level_idx.indices) {
      lev(i) = df.filter(col("初始评级").equalTo(level_idx(i)))
    }

    //借款类型的数据划分
    val kind_idx = Array("电商", "APP闪电", "普通", "其他")
    val kind = new Array[Object](kind_idx.length)
    for (i <- kind_idx.indices) {
      kind(i) = df.filter(col("借款类型").equalTo(kind_idx(i)))
    }

    // 定义借款金额的分组标签
    val amount_idx = Array("0-2000", "2000-3000", "3000-4000", "4000-5000", "5000-6000", "6000+")
    // 使用 when 表达式对借款金额进行分组
    val amountGroup = when(col("借款金额").between(0, 2000), amount_idx(0))
      .when(col("借款金额").between(2000, 3000), amount_idx(1))
      .when(col("借款金额").between(3000, 4000), amount_idx(2))
      .when(col("借款金额").between(4000, 5000), amount_idx(3))
      .when(col("借款金额").between(5000, 6000), amount_idx(4))
      .otherwise(amount_idx(5)) // 6000+

    //逾期还款率的分析
    var dfWithDepayRate = df.withColumn("逾期还款率", col("历史逾期还款期数")
      .divide(col("历史逾期还款期数").plus(col("历史正常还款期数")))
      .multiply(100))
    // 将逾期还款率保留三位小数
    dfWithDepayRate = dfWithDepayRate.withColumn("逾期还款率", format_number(col("逾期还款率"), 3)).na.drop
    //产生新的数据集
    val resDF = dfWithDepayRate.select(col("借款类型"), col("借款金额"), col("逾期还款率"))

    // 按借款类型聚合，计算最高逾期还款率
    val max_resDF = resDF.groupBy(col("借款类型"))
      .agg(functions.max(col("逾期还款率")).alias("最高逾期还款率"))

    // 保存
    max_resDF.write
      .format("csv")
      .option("header", true)
      .save(BaseUrl + "/JucRes/9.按借款类型分析逾期还款率")

    //释放
    spark.close()
  }

}
