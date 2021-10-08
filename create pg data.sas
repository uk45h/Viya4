cas mysess;
caslib _all_ assign;
libname cdp postgres server="sas-crunchy-data-postgres" user=dbmsowner 
	pw=hEQPlymeziWgyCydYMomGZxwpUZlDOtu database=postgres schema=public;

caslib dataproc drop;
caslib dataproc datasource=(srctype=path) path="/mnt/viya-share/megacorp";

libname libcas cas caslib="dataproc" ;
libname pgsas '/mnt/viya-share/pgdata';

caslib pgcaslib desc='PostgreSQL Caslib' 
     dataSource=(srctype='postgres'
                 server='sas-crunchy-data-postgres'
                 authdomain='pglocal'
                 database="postgres"
					schema="public"
                 );

proc casutil;
	   list files incaslib="pgcaslib" ;
	   list files incaslib="dataproc" ;
quit ;

proc casutil ;
   load casdata="customers.sashdat" incaslib="dataproc" outcaslib="dataproc" casout="customers" copies=0 replace ;
quit ;

proc fedsql sessref=mysess;
	create table pgcaslib.customers as
		select * from dataproc.customers;
quit;

libname pglib cas caslib=pgcaslib;
data pglib.debt;
	set sasbank.dane_windykacja1;
run;
proc copy in=pgsas out=cdp memtype=data;
run;
proc casutil;
/*     save casdata="DEBT" incaslib="pgcaslib" outcaslib="pgcaslib" */
/* 	     casout="DEBT_TEST" replace; */

	save casdata="CUSTOMERS" incaslib="pgcaslib" outcaslib="pgcaslib"
	     casout="CUSTOMERS" replace;
quit;

cas mysess terminate;