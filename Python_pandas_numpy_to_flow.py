import numpy as np

input_table = 'work.CARS_2021'
output_table = 'WORK.CARS_2021_PY'

dfin = SAS.sd2df(input_table)

# create a list of our conditions
conditions = [
    (dfin['MSRP_2021'] <= 20000),
    (dfin['MSRP_2021'] > 20000) & (dfin['MSRP_2021'] <= 40000),
    (dfin['MSRP_2021'] > 40000)
    ]

# create a list of the values we want to assign for each condition
values = ['Klasa 1', 'Klasa 2', 'Klasa 3']

# create a new column and use np.select to assign values to it using our lists as arguments
dfin['tier'] = np.select(conditions, values)


SAS.df2sd(dfin, output_table)

SAS.submit('proc print  data=work.CARS_2021_PY;run;')