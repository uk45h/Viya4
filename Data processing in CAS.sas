/* cas mySession terminate; */
cas mySession sessopts=(metrics=true messagelevel=all) ;

%put My Userid is: &sysuserid ;

options msglevel=i ;

caslib dataproc datasource=(srctype=path) path="/mnt/viya-share/megacorp/";

libname libcas cas caslib="dataproc" ;

/* List source files */
proc casutil ;
   list files incaslib="dataproc" ;
quit ;

/* Load tables */
proc casutil ;
   load casdata="megacorp_facts.sashdat" incaslib="dataproc" outcaslib="dataproc" casout="facts" copies=0 replace ;
   load casdata="megacorp_proddim.sashdat" incaslib="dataproc" outcaslib="dataproc" casout="proddim" copies=0 replace ;
   list tables incaslib="dataproc" ;
quit ;

proc casutil ;
   contents casdata="facts" incaslib="dataproc" ;
   contents casdata="proddim" incaslib="dataproc" ;
quit ;

proc cas;
   table.tabledetails status=s/ name="facts" caslib="dataproc" level="node" ;
	if s.severity != 0 then do;
		exit({severity=5,reason=5,statusCode=5});
	end;
	else do;
   		table.tabledetails / name="proddim" caslib="dataproc" level="node" ;
	end;
quit ;

/* Data Step Merge */
data libcas.merge_ds(copies=0) ;
   merge libcas.facts(in=a) libcas.proddim ;
   by ProductID ;
   if a ;
run ;


/* Data Step by groups and first. last. notation */
data libcas.dsby(copies=0 drop=expenses) ;
   length sum_expenses 8 ;
   retain sum_expenses 0 ;
   set libcas.merge_ds(keep=Product expenses) ;
   by Product ;
   if first.Product then sum_expenses=0 ;
   sum_expenses=sum_expenses+expenses ;
   if last.Product then output ;
run;


/* FedSQL Join and Aggregation */
proc fedsql sessref=mySession _method ;
   create table dataproc.join_agg_fed {options replace=true replication=0} as
   select Date, Product, sum(Revenue) as Revenue
   from dataproc.facts as a
      left join dataproc.proddim as b
      on a.ProductID=b.ProductID
   group by Date, Product ;
quit ;

/* Sample RDBMS */

caslib pg desc='PostgreSQL Caslib' 
     dataSource=(srctype='postgres'
                 server='sas-crunchy-data-postgres'
                 authdomain='pglocal'
                 database="postgres"
					schema="public"
                 );

proc fedsql sessref=mySession _method ;
   create table dataproc.fed_pt{options replace=true replication=0} as
   select customer.first_name, customer.last_name, address.address
   from pg."customer" as customer, pg."address" as address where customer.address_id=address.address_id ;
quit ;


/* Table Transposition */
proc cas ;
   action transpose.transpose /
      table={caslib="dataproc",name="join_agg_fed",groupby={"Date"}}
      transpose={"revenue"}
      id={"Product"}
      casOut={caslib="dataproc",name="agg_tr",replace=true,replication=0} ;
quit ;


/* Create format */
proc format casfmtlib="userformats1" ;
   value myDayName
               1="Sunday"
               2="Monday"
               3="Tuesday"
               4="Wednesday"
               5="Thursday"
               6="Friday"
               7="Saturday" ;
run ;

/* List formats */
cas mySession listfmtsearch ;
cas mySession listformats members ;

/* Apply format */
data libcas.facts_fmt(copies=0) ;
   length DayName $ 9 ;
   set libcas.facts(keep=Date DayOfWeek FacilityId FacilityCity ProductId Revenue Profit Expenses) ;
   DayName=put(DayOfWeek,myDayName.) ;
run ;

cas mySession terminate ;
