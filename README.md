## Flink Streaming
>> Mysqlwork.py 在mysql数据库中建立多个表并生成数据。可根据需求调配数据量，亿级数据将更好展现flink，elasticsearcg等工具的性能
+ HelloWorldCount : 简单的测试文件，将会每次打印Hello World

+ MysqlToElasticsearch : 将mysql数据搬到es中，flink使用StreamTableEnvironment数据流方式将数据持久化输出到elasticsearch
  此表有数据量1亿，程序开启一直运行，原有表数据处理完之后将等待新的upsert操作读取数据。缺点：没有checkpoint





## 常见错误处理
+ ```Provided trait [BEFORE_AND_AFTER] can't satisfy required trait [ONLY_UPDATE_AFTER]. This is a bug in planner, please file an issue.```
    ##### 这是flink1.11.0版本的bug，如果将一个change-log的数据源insert到一个upsert sink时就会抛出异常。flink在1.11.1中版本修复了此bug，在pom.xml文件中将flink.version更换成1.11.1即可