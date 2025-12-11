# **📊 交易后置分析框架 (Transaction Post-Analysis Framework)**

**版本**: V1.0 | **作者**: Charles

### **🌐 语言 / Language**

[**中文**](README.md) | [English](README_EN.md)

## **📖 项目简介**

**交易后置分析框架** 是一个基于 SQL 和 Power BI 的数据分析解决方案，旨在处理复杂的交易数据流。通过分层架构（ODS \-\> DWD \-\> ADS），该系统能够自动化执行每日概览、深度商户分析、风险监控及月度趋势预测，帮助业务团队快速识别交易异常与增长机会。

## **🏗 系统架构**

本系统采用标准化的数据处理流，从原始数据摄入到最终的可视化展示，流程如下：
![](/img/png1.png "annotation")


## **🛠 技术栈**

| 组件 | 描述 | 
| ---------- | ---------- |
| 数据库 | MySQL 9.5 |  
| SQL 标准 | ANSI SQL (利用 Window Functions 进行复杂计算) |  
| 可视化 | Power BI (通过 ODBC 连接) / Excel 输出 |  
| 脚本语言 | Shell (自动化调度), Python (数据转换/导入) |

## **📦 核心功能模块**

### **1\. 每日概览 (Daily Overview) ☀️**

* **交易监控**: 实时监控每日交易总量、成功率与平均耗时。  
* **排名分析**: 动态生成商户交易量排名。  
* **随机抽检**: 基于规则（失败码、超时、极端金额）自动抽样近24小时异常订单。

### **2\. 深度分析 (Deep Analysis) 🔍**

* **特定商户行为**: 深入分析特定商户的交易漏斗与失败原因。  
* **用户画像**: 识别用户交易模式，进行风险评分与黑名单交叉验证。

### **3\. 月度趋势 (Monthly Trends) 📈**

* **趋势识别**: 分析长期交易趋势与季节性波动。  
* **异常检测**: 识别并预警异常的数据波动。

### **4\. Power BI 集成 📊**

* **交互式仪表板**: 预定义的数据视图，支持实时刷新与多维度钻取。

## **📂 项目目录结构**

sql-transaction-analysis/  
├── sql/                        \# 🧠 核心 SQL 脚本库  
│   ├── daily\_overview/         \# \[日常\] 概览、趋势、抽检分析  
│   ├── deep\_analysis/          \# \[深度\] 商户与用户专项分析  
│   ├── monthly\_trends/         \# \[月度\] 长期趋势报告  
│   ├── power\_bi\_integration/   \# \[BI\] 对接 Power BI 的视图层  
│   └── setup/                  \# \[基础\] 建表、索引与测试数据  
├── transform_example.py        \# 🤖 自动化python执行脚本 (由excel表导入MySQL数据库)  
└── README.md                   \# 项目说明

## **🗃 数据模型分层**

系统遵循数仓分层设计原则，确保数据清晰可溯源：

### **ODS 层 (原始数据层)**

* ods\_pagsmile\_orders\_raw: 原始订单数据  
* ods\_transfersmile\_payouts\_raw: 原始打款数据

### **DWD 层 (明细数据层)**

* dwd\_payin\_orders\_d: 标准化后的入款表（补充状态语义、身份清洗、时长指标）  
* dwd\_payout\_orders\_d: 标准化后的出款表（补充费率计算、到账时效）

## **🚀 快速开始**

### **1\. 数据准备**

导入原始数据，并配置 Python 脚本中的本地路径：

python scripts/transform.py

### **2\. 初始化环境**

建立基础汇率表（如需）：

\-- 运行 sql/setup/ 目录下的脚本  
SOURCE sql/setup/create\_tables.sql;  
SOURCE sql/setup/fx\_rates.sql; \-- 插入或导入每日汇率

### **3\. 构建视图与模型**

初始化兼容视图与 DWD 层：

SOURCE sql/shared/compat\_views.sql;  
SOURCE sql/shared/dwd\_views.sql;

### **4\. 执行分析任务**

根据业务需求运行对应的 SQL 任务脚本（Task 1 \- Task 8）：

* **例：** 运行 sql/daily\_overview/task1\_overview\_report.sql 获取今日概览。  
* 结果可通过 MySQL Workbench 导出为 .xlsx。

### **5\. Power BI 可视化**

1. 执行 sql/power\_bi\_integration/ 下的脚本创建 BI 专用视图。  
2. 打开 Power BI Desktop，配置 ODBC 连接。  
3. 点击 **刷新 (Refresh)** 即可加载最新分析数据。

## **✅ 测试与验证**

运行测试脚本以确保数据完整性：

SOURCE tests/test\_queries.sql;  
SOURCE tests/test\_data\_validation.sql;

*Created by Charles*
