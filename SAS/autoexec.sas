/*

Do not modify value of any variable beginning with "SAS_" prefix. Such
modifications will be reset to a different value during the
build process. 

The only exception is SAS_HOME. This variable should be configured
as an operating system environment variable. The value should point to
the sas installation directory.

*/

%let SAS_REPORT_HOME=C:/wildfly-8.2.0.Final/nedssdomain/Nedss/report;

libname rdbdata  "&SAS_REPORT_HOME/dw/etl/rdbdata";
filename RPTUTIL "&SAS_REPORT_HOME/util";
filename NBSPGM  "&SAS_REPORT_HOME/pgm";
filename etlpgm  "&SAS_REPORT_HOME/dw/etl/src";
libname nbsfmt   "&SAS_REPORT_HOME/format";
libname library  "&SAS_REPORT_HOME/metadata";

/*
%let SAS_NEDSS_LINK=D:/SAS/inetsrv;
libname dest "&SAS_NEDSS_LINK/NEDSS_DATA";
*/

/* 

The value for SAS_REPORT_DBTYPE must be set manually for this instance of NBS.
After running the NBS build script, modify the deployed instance of autoexec.sas
This file can be found at <Root>/<weblogic directory>/nedssproject/<deployedDomain>/Nedss/report.
*/

%let SAS_REPORT_DBTYPE=SQL SERVER;
/*%let SAS_REPORT_DBTYPE=ORACLE;*/

%macro rptlib ;
%if &SAS_REPORT_DBTYPE=ORACLE %then %do;
  libname nbs_ods oracle user=nbs_ods password=ods path=nbsdb schema=nbs_odse ACCESS=READONLY;
  libname nbs_rdb oracle user=nbs_rdb password=rdb path=nbs_rdb ACCESS=READONLY;
  %end;
%else %do;
  libname nbs_ods ODBC DSN=nedss1 UID=nbs_ods PASSWORD=ods ACCESS=READONLY;
  libname nbs_rdb ODBC DSN=nbs_rdb UID=nbs_rdb PASSWORD=rdb ACCESS=READONLY;
  %end;
%mend rptlib;

%macro etllib;
%if &SAS_REPORT_DBTYPE=ORACLE %then
	%do;
  libname nbs_ods oracle user=nbs_ods password=ods path=nbsdb schema=nbs_odse;
  libname nbs_srt oracle user=nbs_ods password=ods path=nbsdb schema=nbs_srte ACCESS=READONLY;
  libname nbs_rdb oracle user=nbs_rdb password=rdb path=nbs_rdb  schema=nbs_rdb;
%end;
%else
	%do;
  libname nbs_ods ODBC DSN=nedss1 UID=nbs_ods PASSWORD=ods;
  libname nbs_srt ODBC DSN=nbs_srt UID=nbs_ods PASSWORD=ods ACCESS=READONLY;
  libname nbs_rdb ODBC DSN=nbs_rdb UID=nbs_rdb PASSWORD=rdb;
%end;
%mend etllib;


/*
The Event_Metrics data mart is created upon execution of the MasterETL.bat by the Event_Metrics.sas procedure.  
Out of the box, the Event_Metrics data mart will include records created (Event_Metrics.add_time) within the past 
730 days.  To reduce or expand the number of records returned, a state can modify the 
time period by changing the number of days ie., value of variable METRICS_GOBACKBY_DAYS in this file.  
Note: the value for METRICS_GOBACKBY_DAYS cannot be set to NULL or blank, a numeric value must be present.
*/

%let METRICS_GOBACKBY_DAYS= 730;
%let ETL_HEALTHCHECK_GOBACK_DAYS= 20;
%let ETL_LAB_GOBACK_DAYS=1050;
