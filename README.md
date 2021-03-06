# StatisticalSample

## 数据统计示例说明

本示例目的主要是搭建一个利用`pandas`进行数据统计的例程。

统计涉及普通的数据类型，如：时间、数字、字符串等。为了节约存储，时间区间可能就被设计成起始时间和结束时间进行标记，而其他数据类型也可能会设计成 csv 进行存储。但是这样节约空间性设计会给统计带来不小的挑战。在此之前，曾经在.NET 平台用纯 SQL 进行统计，统计结果正确，但是遇到海量数据难免给数据库造成不小的压力。本来打算将统计过程迁移至内存，利用 Linq 进行统计会好很多。那个时候.net core 未曾爆发，难以进行 Linux 服务部署，要不然也不会有后面 Java 的故事。之后便将其迁移至 Java 平台，利用 stream 进行统计。虽然 Java 运行良好，但老是觉得不够优雅。贼心不死，想着利用 Python 优雅的进行实现。基于以上种种，我们开始了新一轮挑战。

本示例优先完成数据统计，后期再考虑性能上的优化，如有可能，将使用分布式平台`Spark`作为更高的性能优化。

## 数据统计示例

统计数据：

| index | start_time          | end_time            | equipment                 | item                   |
| ----- | ------------------- | ------------------- | ------------------------- | ---------------------- |
| 0     | 2019-01-01 01:20:46 | 2019-01-01 07:22:46 | bicycle,computer,swimsuit | swim,basketball,gaming |
| 1     | 2019-01-01 00:20:26 | 2019-01-01 08:24:20 | swimsuit,bicycle          | riding,swim,gaming     |
| 2     | 2019-01-01 00:10:04 | 2019-01-01 05:26:34 |                           | riding,swim,gaming     |

假设我们统计在时间区间`2019-01-01 00:00:00`至`2019-01-01 01:30:00`之间的数据，统计要求为每小时的 item 次数，统计结果如下：

```csv
item    swim  riding  gaming  basketball
time
[0, 1)   2.0     2.0     2.0         0.0
[1, 2)   3.0     2.0     3.0         1.0
```

## 示例特性

- 时间区间统计
- csv 统计(按拆分 csv 后的数据进行)
- 时间过滤
- csv 过滤

## 开发

### 示例

当前示例可以在`test`中找到，具体模块位于`test.statisticin.py`中。

### 软件依赖

- python3.6
- numpy 支持高端大量的维度数组与矩阵运算，此外也针对数组运算提供大量的数学函数库。
- pandas 基于 NumPy 的一种工具，该工具是为了解决数据分析任务而创建的。
- peewee python 下的轻量级 ORM 框架
- cacheout 神奇的本地缓存库

### 数据库

目前使用的是`peewee`映射到数据库数据库。配置文件位于`statistical.conf.database_conf.py`模块里。测试 postgres 、mysql 和 sqlite 均可用。目前好像不支持 oracle，不过，作为示例，这已经足够了。

## roadmap

- 数据框架搭建（完成）
- 基本数据统计（完成）
- 统计数据过滤（完成）
- Spark 使用（基本完成，性能优化中。。。）
- 统计性能优化（持续进行中。。。）

## performance

测试机器：

- model name : Intel(R) Core(TM) i5 CPU @ 2.60GHz
- cache size : 3072 KB
- siblings : 4

测试数据量：

- 100_000 条随机数据
- 单机docker的postgres数据库中

测试条件：

- 时间条件：TimeParameter().segmentation = 'H'
- 过滤条件：

  ```python
  filter_dic = {
             'equipment': ['bicycle'],#7
             'item': ['swim', 'riding', 'gaming']#10,11,12
         }
  ```

### Pandas测试结果：

#### 2019-5-20：

1. 单线程约 60s(内存约300Mb)
2. 2 个进程 30s（速度并没有随进程数显著增加，虽然 cpu 会占满，4进程24s左右）
3. 运行时间跟统计条件输入有很大的关系。比如不统计时间维度（`TimeParameter().segmentation = ''`），那么时间将到达秒级

#### 2019-5-25：

优化策略：迁移spark使用中的经验，替换使用在DataFrame上的for迭代

1. 单线程约 27s
2. 4 个进程 13s

### Spark测试结果：

#### 2019-5-25：

1. local[4]约27s（cpu全满，内存约900Mb。有待优化）