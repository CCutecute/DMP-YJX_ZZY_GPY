CREATE TABLE release.sync_mysql_hive (
  `id` bigint(32) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `db_mysql` varchar(30) NOT NULL DEFAULT 'shop' COMMENT 'mysql数据库',
  `table_mysql` varchar(50) NOT NULL COMMENT 'mysql表',
  `db_hive` varchar(30) NOT NULL DEFAULT 'ods_shop' COMMENT 'hive数据库',
  `table_hive` varchar(50) NOT NULL COMMENT 'hive表',
  `save_mode` varchar(10) NOT NULL DEFAULT 'overwrite' COMMENT '写入方式overwrite|append',
  `status` int NOT NULL DEFAULT 1 COMMENT '有效1|无效0',
  `fieldMap` varchar(100) NOt NULL DEFAULT '' COMMENT '转换字段',
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_mysql_hive` (`db_mysql`,`table_mysql`,`db_hive`,`table_hive`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


//==========================================================

insert into release.sync_mysql_hive(db_mysql, table_mysql, db_hive, table_hive)
values('release','dm_customer_sources','dm_release','dm_customer_sources');

