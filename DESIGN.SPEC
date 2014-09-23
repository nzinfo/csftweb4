Object Space v1 设计 Spec

1 对象实际的数据存储 LedisDB
2 对外的操作接口 Python Restful


场景
============
[同步数据表]
1 配置好数据库的读取方式， 包括
 	数据库 到 LedisDB 的 数据库编号的 映射关系
 		* 在数据表 0 存储 meta 信息
 	访问数据库需要的信息： 地址 端口 用户名 密码 数据库名
 	定义在 JSON ？ Hash Dict？

2 同步数据库的表结构定义
 	数据表 的 类 JSON 定义形式
 	如果 JSON 已经存在，则覆盖
 	python manager.py sync schema [database_name]
 	Python manager.py schema [database_name] [table_name] 查看表结构的 JSON 文件 （是否格式化？）


[增加实际的 worker ]
1 安装好 indexer search searchd mmseg
2 设置 中心控制的元数据库服务器
	python manager.py config meta <the meta host address>

  * 本服务器会下发 数据服务器的配置信息
  * 中心服务器会告知其公钥信息， 客户端应自动完成无 key 的登录

3 配置节点名称， 如果没有额外的配置，使用档期主机的 hostname
	python manager.py config hostname <the host name>

3 配置存放生成的配置文件的路径 & 日志文件的路径
	日志文件名的构成是：
		日志类型 + <port> + <search_node_hostname> . postfix

4 加入到集群
	channel 是 对应要参与协作的 索引频道的名字 ， 在 同样的 端口上 ， 可以加入多个 频道
	每定义一个端口， 都对应要启动一个 searchd
	python manager.py join channel port

  查看当前的机器 都 在运行那些 channel
  	python manager.py status
  	python manager.py status channel_name

  	port, [channelA, channelB] , status, pid

  	status 包括： serving | rotating | indexing | sync
  		sync 出现在 从节点 同步 primary_node 的索引数据时

  	需要查询 meta 节点， 得到 当前节点加入了那些 channel，

5 离开集群
	python manager.py leave channel
	不需要 提供端口

6 搜索服务启停
	python manager.py start
	python manager.py start channel_name

	python manager.py stop
	python manager.py stop channel_name

7 构建索引
	理论上，应该由 meta 激发
	依赖： 查看索引的分配

	python manager.py index query [channel]
	查看 本机被分配了channel中索引的哪几部分

	python manager.py index build [channel] [index_name]
	构造索引


8 注册为Listener模式
	- 启动 Web 服务
	- 只允许 meta 访问
	- 随机生成 password
	- 告知 meta password
	等待 meta 的 http 接口的回调

    python manager.py runserver

9 服务器端可用的指令
	- 更新 killlist
	  告知 Primary Shard 新的 KillList 文件在那里读取，
	- 重建索引
	  重新从数据节点读取数据， （额外的）可以在此给出数据节点的地址
	- 查询状态
	- 启动
	- 停止

[数据的Shard]
1 可以配置使用 docid 做 Shard 还是取某属性做 Shard
2 系统内部有一个全局的 docid 序列

[定义表关系]
为了简化开发， 直接采用 JSON
1 python manager.py schema table_name get 读取 JSON 的表定义
2 python manager.py schema table_name put 更新 JSON 的表定义， Read from stdin {JSON_DEFINE}
3 定义表的最后一个修改字段的...

[定义索引]
  因为表关系已经在之前定义，需要定义
  1 索引
  2 索引都有那些表
  3 索引对应的字段类型 <- 根据原始表推导； 如果没有，则忽略； 可以指定过滤器

  python manager.py channel [name]	如果没有名字，为列出全部的 channel； 如果有名字， 为输出 Channel 定义
  	- 包括那些表；
  	- 全文检索字段的定义；
  	- 索引的定义：
  		包括多少个 primary_node, second_node
  		包括多个分区

[集群管理]
  查询集群有关的配置信息 & 统计信息  （统计信息暂时没有）
  1 查询集群的节点运行情况
    python manager.py cluster status
    - 当前多少个 node
    - 多少个 channel (索引频道)
    - channel 在各 node 上的分布状态
  2 封装对 Sphinx 的调用，通过 MySQL 协议
  3 权限的控制？
  4 “假数据” How to？



[同步数据-全量同步]
1 执行命令
	python manager.py sync
	- 执行命令时， 会清空数据库的全部记录
	- 同步前记录文档主键的最大值
	- 同步后，重新检查文档的一致性 （使用事务）
	- ？记录同步的状态？

[同步数据-增量同步]
1 执行命令
	python manager.py resync 从最后一次同步的增量开始
	- 通过最后一次修改时间记录
	- 通过日志查询
	- 通过 binlog

2 可以设置 rsync 的策略， 每个表不同  （因为 实际数据中有原始值，因此额外记录原始值是无用的）
	- 时间戳：
		* 需要指明哪一个字段为最后修改时间
		python manager.py sync_policy policy:[lastmodify|log] database tablename field_name
	- 日志
		* 系统自建日志表
			+ 如果不能自动生成，则给出创建的 SQL
			+ 日志表结构 （sn， 时间， 表名（have index）， 主键JSON, op_code(ins|upd|del)）
	- binlog
		* 数据库级同步
		* 暂不支持

3 可以设置 rsync 同步的时间间隔， 如果同一个表已经有一个同步进程在执行，则退出。
	- node_name, pid, start_at
	- 提供选项，可以无视同步进程
	- 可以强制取代？
	* 目前， 同步的过程必须固定在某台机器上， 不能在集群中随便转


FIXME
==============
1 Rotate Index 时， 需要重新加载 MMSeg 的词库
2 如果可能 Rotate 只更新 KillList
















