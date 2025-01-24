import pandas as pd
from hdfs import InsecureClient

# 计算借款成功率
def fun1(hdfs_path, hdfs_url, user, column_name):
    """
    向 HDFS 中的 CSV 文件添加一列，并根据“历史成功借款次数”动态设置该列的值。

    :param hdfs_path: HDFS 文件路径，例如 "/SourceData/juc2or_20k.csv"
    :param hdfs_url: HDFS 的 WebHDFS URL，例如 "http://192.168.49.128:50070"
    :param user: HDFS 用户名，例如 "hadoop"
    :param column_name: 要添加的列名，例如 "借款成功率"
    """
    # 创建 HDFS 客户端
    client = InsecureClient(hdfs_url, user=user)

    # 从 HDFS 读取 CSV 文件
    with client.read(hdfs_path) as reader:
        df = pd.read_csv(reader)

    # 添加新列并根据“历史成功借款次数”动态设置值
    df[column_name] = df["历史成功借款次数"].apply(lambda x: 0 if x == 0 else 1)

    # 将修改后的 DataFrame 写回 HDFS
    with client.write(hdfs_path, overwrite=True) as writer:
        df.to_csv(writer, index=False)

    print(f"成功向 {hdfs_path} 添加列 {column_name}，并根据“历史成功借款次数”设置值")


fun1(
    hdfs_path="/SourceData/juc2or_20k.csv",
    hdfs_url="http://192.168.49.128:50070",
    user="hadoop",
    column_name="借款成功率"
)

# 计算历史逾期率
def add_historical_overdue_rate_to_hdfs_csv(hdfs_path, hdfs_url, user, new_column_name):
    """
    向 HDFS 中的 CSV 文件添加一列，计算历史逾期率并设置该列的值。

    :param hdfs_path: HDFS 文件路径，例如 "/SourceData/juc2or_20k.csv"
    :param hdfs_url: HDFS 的 WebHDFS URL，例如 "http://192.168.49.128:50070"
    :param user: HDFS 用户名，例如 "hadoop"
    :param new_column_name: 要添加的列名，例如 "历史逾期率"
    """
    # 创建 HDFS 客户端
    client = InsecureClient(hdfs_url, user=user)

    # 从 HDFS 读取 CSV 文件
    with client.read(hdfs_path) as reader:
        df = pd.read_csv(reader)

    # 计算历史逾期率
    df[new_column_name] = (df["历史逾期还款期数"] / (df["历史逾期还款期数"] + df["历史正常还款期数"])) * 100

    # 将修改后的 DataFrame 写回 HDFS
    with client.write(hdfs_path, overwrite=True) as writer:
        df.to_csv(writer, index=False)

    print(f"成功向 {hdfs_path} 添加列 {new_column_name}，并计算历史逾期率")

# 示例调用
add_historical_overdue_rate_to_hdfs_csv(
    hdfs_path="/SourceData/juc2or_20k.csv",
    hdfs_url="http://192.168.49.128:50070",
    user="hadoop",
    new_column_name="历史逾期率"
)

