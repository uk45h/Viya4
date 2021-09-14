data test;
	set sashelp.class(where=(sex='F' or sex='M'));
run;