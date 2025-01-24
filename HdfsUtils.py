import pandas as pd
from hdfs import InsecureClient

def read_hdfs(hdfs_file,host,username):
    # host http://192.168.49.128:50070
    # user hadoop
    client = InsecureClient(host,username)
    with client.read(hdfs_file) as reader:
        data = reader.read()
    return data
    
def read_csv_hdfs(hdfs_dir, host, username):
    # 创建 HDFS 客户端
    client = InsecureClient(host, username)
    
    # 获取 hdfs_dir 下所有文件，筛选出 .csv 文件
    files = [f for f in client.list(hdfs_dir, status=False) if f.endswith('.csv')]
    df_list = []

    # 读取每个 CSV 文件并存入列表
    for file in files:
        with client.read(hdfs_dir + '/' + file) as reader:
            df_list.append(pd.read_csv(reader))
    
    # 合并所有 CSV 文件
    df_merged = pd.concat(df_list, ignore_index=True)
    return df_merged

def save_to_local(df, local_path):
    # 将 DataFrame 保存为本地 CSV 文件
    df.to_csv(local_path, index=False, encoding='utf-8')
    print(f"文件已保存到本地：{local_path}")

if __name__ == '__main__':
    # 从 HDFS 读取数据
    df = read_csv_hdfs("/JucRes/9.按借款类型分析逾期还款率", "http://192.168.49.128:50070", "hadoop")
    
    # 将读取的数据保存到本地
    local_file_path = "D:/edu/shixun4/JucRes/"  # 本地保存路径
    save_to_local(df, local_file_path + "9.按借款类型分析逾期还款率.csv")