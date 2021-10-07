cas mySession sessopts=(metrics=true messagelevel=all) ;

%put My Userid is: &sysuserid ;

options msglevel=i ;

caslib dataproc datasource=(srctype=path) path="/gelcontent/demo/DM/data/SAMPLE" ;

libname libcas cas caslib="dataproc" ;

/* Load the customers table from the new dataproc caslib */
proc casutil ;
   load casdata="customers.sashdat" incaslib="dataproc" outcaslib="dataproc" 
		casout="&sysuserid._customers" copies=0 replace ;
quit ;

data libcas.&sysuserid._customers_dq(copies=0) ;
   length mcName mcAddress stdName varchar(50) stdState $ 2 ;
   set libcas.&sysuserid._customers ;
   stdState=dqStandardize(state,'State/Province (Abbreviation)','ENUSA');
   stdName=dqStandardize(name,'Name','ENUSA');
   mcName=dqMatch(name,'Name',85,'ENUSA') ;
   mcAddress=dqMatch(address,'Address (Street Only)',85,'ENUSA') ;
run ;

/* Clustering */
proc cas ;
   entityRes.match /
      clusterId="clusterID"
      inTable={caslib="dataproc",name="&sysuserid._customers_dq"}
      columns={"stdName","address","mcName","mcAddress"}
      matchRules={{
         rule = { { columns = { "mcName" , "mcAddress" } } }
      }}
      nullValuesMatch=false
      emptyStringIsNull=true
      outTable={caslib="dataproc",name="&sysuserid._customers_clustered",replace=true} ;
quit ;


/* Profile and Identity analysis */
proc cas;
   dataDiscovery.profile /
      algorithm="PRIMARY"
      table={caslib="dataproc" name="&sysuserid._customers"}
      columns={"state"}
      multiIdentity=true
      locale="ENUSA"
      qkb="QKB CI 32"
      identities= {
         {pattern=".*", type="*", definition="Field Content", prefix="QKB_"}
      }
      cutoff=20
      frequencies=10
      outliers=5
      casOut={caslib="dataproc" name="&sysuserid._customers_profiled" replace=true replication=0}
   ;
   table.fetch /
      table={caslib="dataproc" name="&sysuserid._customers_profiled"} to=200 ;
quit ;

cas mySession terminate ;