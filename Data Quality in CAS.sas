cas mySession sessopts=(metrics=true messagelevel=all) ;

%put My Userid is: &sysuserid ;

options msglevel=i ;

caslib dataproc datasource=(srctype=path) path="/mnt/viya-share/megacorp" ;

libname libcas cas caslib="dataproc" ;

/* Load the customers table from the new dataproc caslib */
proc casutil ;
   load casdata="customers.sashdat" incaslib="dataproc" outcaslib="dataproc" 
		casout="customers" copies=0 replace ;
quit ;

data libcas.customers_dq(copies=0) ;
   length mcName mcAddress stdName varchar(50) stdState $ 2 ;
   set libcas.customers ;
   stdState=dqStandardize(state,'State/Province (Abbreviation)','ENUSA');
   stdName=dqStandardize(name,'Name','ENUSA');
   mcName=dqMatch(name,'Name',85,'ENUSA') ;
   mcAddress=dqMatch(address,'Address (Street Only)',85,'ENUSA') ;
run ;

/* Clustering */
proc cas ;
   entityRes.match /
      clusterId="clusterID"
      inTable={caslib="dataproc",name="customers_dq"}
      columns={"stdName","address","mcName","mcAddress"}
      matchRules={{
         rule = { { columns = { "mcName" , "mcAddress" } } }
      }}
      nullValuesMatch=false
      emptyStringIsNull=true
      outTable={caslib="dataproc",name="customers_clustered",replace=true} ;
quit ;

data work.customers_dq;
	set libcas.customers_dq;
run;
proc dqmatch data=work.customers_dq out=work.customers_clustered cluster=clusterID;
	criteria condition=1 var=mcName exact;
	criteria condition=1 var=mcAddress exact;
run;
proc sql;
	select count(*) from (
		select clusterID, count(*) as ile 
			from work.customers_clustered 
			where missing(clusterID)=0
			group by 1 
			having count(*)>1
	) as sq;
quit;
proc fedsql sessref=mysession;
	select count(*) from (
		select clusterID, count(*) as ile 
			from dataproc.customers_clustered 
			where missing(clusterID)=0
			group by 1 
			having count(*)>1
	) as sq;
quit;

cas mySession terminate ;