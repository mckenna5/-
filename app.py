import os
import io
import base64
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from io import StringIO
import pandas as pd
from flask import Flask, request, jsonify, render_template
from HdfsUtils import read_csv_hdfs, read_hdfs
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression

app = Flask(__name__)

# 支持中文，创建字体属性对象，用于设置中文显示，用字体管理模块管理字体。
font_path = os.path.join(os.path.dirname(__file__), "static", "fonts", "msyh.ttc")
font_prop = fm.FontProperties(fname=font_path)


# 首页
@app.route("/")
def mypage():
    return render_template("index.html")


# 性别和年龄与金额分析
@app.route("/fun1")
def fun1():
    # 读数据
    sexDf = read_csv_hdfs(
        "/JucRes/2.按性别和年份分析借款金额/", "http://192.168.49.128:50070", "hadoop"
    )
    ageDF = read_csv_hdfs(
        "/JucRes/5.分析年龄段与借款金额的关系/", "http://192.168.49.128:50070", "hadoop"
    )

    # 设置中文字体
    plt.rcParams["font.sans-serif"] = ["SimHei"]

    # 创建第一个图表：按性别分析借款金额
    fig1, ax1 = plt.subplots(figsize=(8, 6))  # 创建一个画布
    ax1.bar(sexDf["性别"].tolist(), sexDf["sum(借款金额)"].tolist(), color="skyblue")
    ax1.set_xlabel("性别(Sex)", fontproperties=font_prop)  # 设置x轴标签
    ax1.set_ylabel("借款金额(Frequency)", fontproperties=font_prop)  # 设置y轴标签
    ax1.set_title("按性别分析借款金额", fontproperties=font_prop)  # 设置标题

    # 将第一个图表转换为 base64 编码
    output1 = io.BytesIO()
    plt.savefig(output1, format="png", bbox_inches="tight")
    output1.seek(0)
    plot_url1 = base64.b64encode(output1.getvalue()).decode()

    # 清除当前图表，避免影响下一个图表
    plt.clf()

    # 创建第二个图表：按年龄段分析借款金额
    fig2, ax2 = plt.subplots(figsize=(10, 6))  # 创建一个画布
    ax2.bar(ageDF["ageIdx"].tolist(), ageDF["总借款金额"].tolist(), color="lightgreen")
    ax2.set_xlabel("年龄区间(Age Group)", fontproperties=font_prop)  # 设置x轴标签
    ax2.set_ylabel(
        "总借款金额(Total Loan Amount)", fontproperties=font_prop
    )  # 设置y轴标签
    ax2.set_title("按年龄段分析借款金额", fontproperties=font_prop)  # 设置标题
    plt.xticks(rotation=45)  # 旋转x轴标签

    # 将第二个图表转换为 base64 编码
    output2 = io.BytesIO()
    plt.savefig(output2, format="png", bbox_inches="tight")
    output2.seek(0)
    plot_url2 = base64.b64encode(output2.getvalue()).decode()

    # 渲染模板并传递图表数据
    return render_template(
        "page1.html", plot_url1=plot_url1, plot_url2=plot_url2, charset="utf-8"
    )


# 历史借款次数和金额分析
@app.route("/fun2")
def fun2():
    # 读取数据
    data_bytes = read_hdfs(
        "/data/juc2-clear.csv", "http://192.168.49.128:50070", "hadoop"
    )

    # 将字节数据解码为字符串
    data_str = data_bytes.decode("utf-8")

    # 将字符串转换为 Pandas DataFrame

    df = pd.read_csv(StringIO(data_str))

    # 检查并解码列名
    df.columns = [
        col.decode("utf-8") if isinstance(col, bytes) else col for col in df.columns
    ]

    # 确保 '历史成功借款金额' 列是数值类型
    df["历史成功借款金额"] = pd.to_numeric(df["历史成功借款金额"], errors="coerce")

    # 删除空值
    df = df.dropna(subset=["历史成功借款金额"])

    # 绘制借款人历史成功借款次数和金额的散点图
    fig, ax = plt.subplots(figsize=(8, 6))  # 创建一个画布
    ax.scatter(df["历史成功借款次数"], df["历史成功借款金额"])  # 绘制散点图
    ax.set_xlabel("历史成功借款次数(Count)", fontproperties=font_prop)  # 设置x轴标签
    ax.set_ylabel("历史成功借款金额(Amount)", fontproperties=font_prop)  # 设置y轴标签
    ax.set_title("借款人历史成功借款次数和金额", fontproperties=font_prop)  # 设置标题
    output = io.BytesIO()  # 创建一个io对象
    plt.savefig(output, format="png", bbox_inches="tight")  # 保存图片
    output.seek(0)  # 设置文件指针为0
    plot_url = base64.b64encode(output.getvalue()).decode()  # base64编码
    plt.close(fig)  # 关闭图表，释放内存

    # 绘制借款人历史成功借款金额的箱线图
    fig2, ax2 = plt.subplots(figsize=(8, 6))  # 创建一个画布
    ax2.boxplot(df["历史成功借款金额"].dropna())  # 绘制箱线图，并忽略空值
    ax2.set_ylabel("历史成功借款金额(Amount)", fontproperties=font_prop)  # 设置y轴标签
    ax2.set_title("借款人历史成功借款金额分布", fontproperties=font_prop)  # 设置标题
    output2 = io.BytesIO()  # 创建一个io对象
    plt.savefig(output2, format="png", bbox_inches="tight")  # 保存图片
    output2.seek(0)  # 设置文件指针为0
    plot_url2 = base64.b64encode(output2.getvalue()).decode()  # base64编码
    plt.close(fig2)  # 关闭图表，释放内存

    return render_template(
        "page2.html", plot_url=plot_url, plot_url2=plot_url2, charset="utf-8"
    )


# 信用评级借款金额分析
@app.route("/fun3")
def fun3():
    # 读取数据
    data_bytes = read_hdfs(
        "/SourceData/juc2or_20k.csv", "http://192.168.49.128:50070", "hadoop"
    )

    # 将字节数据解码为字符串
    data_str = data_bytes.decode("utf-8")

    # 将字符串转换为 Pandas DataFrame
    df = pd.read_csv(StringIO(data_str))

    # 检查并解码列名
    df.columns = [
        col.decode("utf-8") if isinstance(col, bytes) else col for col in df.columns
    ]

    # 过滤掉 'AAA' 评级（如果存在）
    df = df[~df["初始评级"].isin(["AAA"])]

    # 绘制各信用评级借款金额的箱线图
    fig, ax = plt.subplots(figsize=(10, 6))  # 创建一个画布
    df.boxplot(column="借款金额", by="初始评级", vert=False, ax=ax)  # 绘制箱线图
    ax.set_xlabel("借款金额(Amount)", fontproperties=font_prop)  # 设置x轴标签
    ax.set_ylabel("初始评级(Initial Rating)", fontproperties=font_prop)  # 设置y轴标签
    ax.set_title(
        "各信用评级借款金额(Distribution of Borrower Credit Rating)",
        fontproperties=font_prop,
    )  # 设置标题
    output = io.BytesIO()  # 创建一个io对象
    plt.savefig(output, format="png", bbox_inches="tight")  # 保存图片
    output.seek(0)  # 设置文件指针为0
    plot_url = base64.b64encode(output.getvalue()).decode()  # base64编码
    plt.close(fig)  # 关闭图表，释放内存

    # 绘制各信用评级借款金额的堆叠条形图
    fig2, ax2 = plt.subplots(figsize=(10, 6))  # 创建一个画布
    df.groupby("初始评级")["借款金额"].sum().plot(
        kind="bar", stacked=True, ax=ax2
    )  # 绘制堆叠条形图
    ax2.set_xlabel("初始评级(Initial Rating)", fontproperties=font_prop)  # 设置x轴标签
    ax2.set_ylabel("借款金额(Amount)", fontproperties=font_prop)  # 设置y轴标签
    ax2.set_title(
        "各信用评级借款金额(Distribution of Borrower Credit Rating)",
        fontproperties=font_prop,
    )  # 设置标题
    output2 = io.BytesIO()  # 创建一个io对象
    plt.savefig(output2, format="png", bbox_inches="tight")  # 保存图片
    output2.seek(0)  # 设置文件指针为0
    plot_url2 = base64.b64encode(output2.getvalue()).decode()  # base64编码
    plt.close(fig2)  # 关闭图表，释放内存

    return render_template(
        "page3.html", plot_url=plot_url, plot_url2=plot_url2, charset="utf-8"
    )


# 封装模型训练和预测逻辑
def train_and_predict():
    # 从 HDFS 加载数据
    data_bytes = read_hdfs(
        "/SourceData/juc2or_20k.csv", "http://192.168.49.128:50070", "hadoop"
    )
    data_str = data_bytes.decode("utf-8")
    df = pd.read_csv(StringIO(data_str))

    # 定义分类和数值特征列
    categorical_cols = ["初始评级", "借款类型"]
    numeric_cols = ["借款期限", "借款利率"]

    # 创建列转换器
    preprocessor = ColumnTransformer(
        [
            ("cat", OneHotEncoder(), categorical_cols),  # 对分类特征进行独热编码
            ("num", StandardScaler(), numeric_cols),  # 对数值特征进行标准化
        ]
    )

    # 提取特征和目标变量
    x = df[categorical_cols + numeric_cols]
    y = df["借款金额"]

    # 对特征进行转换
    x = preprocessor.fit_transform(x)

    # 构建并训练线性回归模型
    lin_reg = LinearRegression()
    lin_reg.fit(x, y)

    return preprocessor, lin_reg


# 初始化模型和预处理器
preprocessor, lin_reg = train_and_predict()


# 线性回归模型预测借款金额
@app.route("/fun4", methods=["GET", "POST"])
def fun4():
    if request.method == "GET":
        return render_template("page4.html")
    elif request.method == "POST":
        try:
            # 打印请求头和请求体
            print("Request Headers:", request.headers)
            print("Request Data:", request.data)

            # 检查 Content-Type
            if not request.is_json:
                return jsonify({"error": "Content-Type must be application/json"}), 415

            # 接收 JSON 数据
            data = request.json
            print("Parsed JSON Data:", data)

            if not data:
                return jsonify({"error": "No data provided"}), 400

            # 将 JSON 数据转换为 DataFrame
            X_api = pd.json_normalize(data)

            # 检查是否缺少必要字段
            required_fields = ["初始评级", "借款类型", "借款期限", "借款利率"]
            if not all(field in X_api.columns for field in required_fields):
                return jsonify({"error": "Missing required fields"}), 400

            # 对输入数据进行预处理
            X_api = preprocessor.transform(X_api)

            # 使用模型进行预测
            y_pred = lin_reg.predict(X_api)

            # 返回预测结果
            return jsonify({"predicted_amount": y_pred[0]})
        except Exception as e:
            print("Error:", str(e))  # 打印错误信息
            return jsonify({"error": str(e)}), 500


# 预测借款成功率

# 全局变量
model_fun5 = None


# 封装模型训练和预测逻辑
def train_and_predict_fun5():
    # 从 HDFS 加载数据
    data_bytes = read_hdfs(
        "/SourceData/juc2or_20k.csv", "http://192.168.49.128:50070", "hadoop"
    )
    data_str = data_bytes.decode("utf-8")
    df = pd.read_csv(StringIO(data_str))

    # 定义特征和目标变量
    X = df[
        ["年龄", "性别", "手机认证", "淘宝认证"]
    ].copy()  # 使用副本避免 SettingWithCopyWarning
    y = df["借款成功率"]

    # 删除包含缺失值的行
    X.dropna(inplace=True)
    y = y[X.index]

    # 处理性别字段
    X.loc[:, "性别"] = X["性别"].apply(lambda x: 1 if x == "男" else 0)

    # 处理手机认证和淘宝认证字段
    X.loc[:, "手机认证"] = X["手机认证"].apply(lambda x: 1 if x == "成功认证" else 0)
    X.loc[:, "淘宝认证"] = X["淘宝认证"].apply(lambda x: 1 if x == "成功认证" else 0)

    # 构建并训练线性回归模型
    model_fun5 = LinearRegression()
    model_fun5.fit(X, y)

    return model_fun5


model_fun5 = train_and_predict_fun5()


# 定义路由
@app.route("/fun5", methods=["GET", "POST"])
def fun5():
    if request.method == "GET":
        return render_template("page5.html")  # 返回页面
    elif request.method == "POST":
        try:
            # 检查 Content-Type
            if not request.is_json:
                return jsonify({"error": "Content-Type must be application/json"}), 415

            # 接收 JSON 数据
            data = request.json
            if not data:
                return jsonify({"error": "No data provided"}), 400

            # 将 JSON 数据转换为 DataFrame
            new_data = pd.DataFrame(data, index=[0])

            # 处理性别字段
            new_data.loc[:, "性别"] = new_data["性别"].apply(
                lambda x: 1 if x == "男" else 0
            )

            # 处理手机认证和淘宝认证字段
            new_data.loc[:, "手机认证"] = new_data["手机认证"].apply(
                lambda x: 1 if x == "成功认证" else 0
            )
            new_data.loc[:, "淘宝认证"] = new_data["淘宝认证"].apply(
                lambda x: 1 if x == "成功认证" else 0
            )

            # 使用模型进行预测
            pred = model_fun5.predict(
                new_data[["年龄", "性别", "手机认证", "淘宝认证"]]
            )

            # 返回预测结果
            return jsonify({"prediction": pred[0]})
        except Exception as e:
            return jsonify({"error": str(e)}), 500


# 全局变量
model_fun6 = None


# 封装模型训练和预测逻辑
def train_and_predict_fun6():
    try:
        # 从 HDFS 加载数据
        data_bytes = read_hdfs(
            "/SourceData/juc2or_20k.csv", "http://192.168.49.128:50070", "hadoop"
        )
        data_str = data_bytes.decode("utf-8")
        df = pd.read_csv(StringIO(data_str))

        # 定义特征和目标变量
        X = df[
            ["历史成功借款次数", "历史正常还款期数", "历史逾期还款期数"]
        ].copy()  # 使用副本避免 SettingWithCopyWarning
        y = df["历史逾期率"]  # 直接从数据集中读取目标变量

        # 删除包含缺失值的行
        mask = X.notna().all(axis=1) & y.notna()  # 找到特征和目标变量中非缺失值的行
        X = X[mask]  # 删除特征变量中的对应行
        y = y[mask]  # 删除目标变量中的对应行

        # 打印清理后的数据信息
        print("清理后的数据：")
        print(f"特征变量形状: {X.shape}, 目标变量形状: {y.shape}")

        # 构建并训练线性回归模型
        model_fun6 = LinearRegression()
        model_fun6.fit(
            X[["历史成功借款次数", "历史正常还款期数", "历史逾期还款期数"]], y
        )

        return model_fun6
    except Exception as e:
        print(f"模型训练失败: {e}")
        return None


# 训练模型
model_fun6 = train_and_predict_fun6()


# 定义路由
@app.route("/fun6", methods=["GET", "POST"])
def fun6():
    if request.method == "GET":
        return render_template("page6.html")  # 返回页面
    elif request.method == "POST":
        try:
            # 检查 Content-Type
            if not request.is_json:
                return jsonify({"error": "Content-Type must be application/json"}), 415

            # 接收 JSON 数据
            data = request.json
            if not data:
                return jsonify({"error": "No data provided"}), 400

            # 将 JSON 数据转换为 DataFrame
            new_data = pd.DataFrame(
                {
                    "历史成功借款次数": [data["成功借款次数"]],
                    "历史正常还款期数": [data["正常还款期数"]],
                    "历史逾期还款期数": [data["逾期还款期数"]],
                }
            )

            # 使用模型进行预测
            if model_fun6 is not None:
                pred = model_fun6.predict(
                    new_data[
                        ["历史成功借款次数", "历史正常还款期数", "历史逾期还款期数"]
                    ]
                )
                return jsonify({"pred_risk": pred[0]})
            else:
                return jsonify({"error": "模型未训练成功"}), 500
        except Exception as e:
            return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
