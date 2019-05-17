# StatisticalSample

## 开发依赖

### 软件依赖

- python3.6
- numpy 支持高端大量的维度数组与矩阵运算，此外也针对数组运算提供大量的数学函数库。
- pandas pandas 是基于 NumPy 的一种工具，该工具是为了解决数据分析任务而创建的。
- peewee python 下的轻量级 ORM 框架
- cacheout 神奇的本地缓存库

### 数据库

目前使用的是`peewee`映射到数据库数据库，已跟数据库解耦。配置文件位于`Datas.DataBaseConf`模块里。测试postgres和mysql均可用。
