-- Databricks notebook source
drop table if exists demo_db.fire_service_calls_tbl;
drop view if exists demo_db;

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/demo_db.db

-- COMMAND ----------

create database if not exists demo_db

-- COMMAND ----------

create table if not exists demo_db.fire_service_calls_tbl(
  CallNumber integer,
  UnitID string,
  IncidentNumber integer,
  CallType string,
  CallDate string,
  WatchDate string,
  CallFinalDisposition string,
  AvailableDtTm string,
  Address string,
  City string,
  Zipcode integer,
  Battalion string,
  StationArea string,
  Box string,
  OriginalPriority string,
  Priority string,
  FinalPriority integer,
  ALSUnit boolean,
  CallTypeGroup string,
  NumAlarms integer,
  UnitType string,
  UnitSequenceInCallDispatch integer,
  FirePreventionDistrict string,
  SupervisorDistrict string,
  Neighborhood string,
  Location string,
  RowID string,
  Delay float
) using parquet

-- COMMAND ----------

insert into demo_db.fire_service_calls_tbl 
values(1234, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
null, null, null, null, null, null, null, null, null)

-- COMMAND ----------

select * from demo_db.fire_service_calls_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **You can load data into the table using the insert into values command. But that's not the way we load data into Spark tables**
-- MAGIC
-- MAGIC The first thing is to truncate the table, so we clean the garbage record that we just inserted.
-- MAGIC
-- MAGIC *Why truncate?*
-- MAGIC
-- MAGIC Why not delete?
-- MAGIC
-- MAGIC Because Spark SQL doesn't offer to delete statements.
-- MAGIC
-- MAGIC You cannot delete data from a Spark Table using Spark SQL.

-- COMMAND ----------

truncate table demo_db.fire_service_calls_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We created the **sf_fire_service_calls_view** before

-- COMMAND ----------

insert into demo_db.fire_service_calls_tbl
select * from global_temp.sf_fire_service_calls_view

-- COMMAND ----------

select * from demo_db.fire_service_calls_tbl

-- COMMAND ----------


