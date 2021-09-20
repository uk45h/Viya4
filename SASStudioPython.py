import os, sys
sys.path

input_table = 'SASHELP.CLASS'
output_table = 'WORK.PYTHONOUT'

dfin = SAS.sd2df(input_table)

print("input data shape is:", dfin.shape)

dfout = dfin.transpose()

print("output data shae is:", dfout.shape)

SAS.df2sd(dfout, output_table)

SAS.submit('proc print  data=work.pythonout;run;')