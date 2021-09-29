data wykres;
	infile datalines dlm=',';
	input records minutes;	
	records2=records**2;
cards;
16, 36
1881, 34
14984, 48
152087, 139
298893, 233
;
proc sgplot datas=wykres;
	reg y=records x=minutes;
run;
quit;
proc reg data=wykres plots;
	model minutes=records records2; 
run;
quit;