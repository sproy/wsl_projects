import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from datetime import datetime

# preparing data
df = pd.read_csv('./MMDC/confirmation_production.csv', sep=',')
df.rename(columns={"TO_CHAR(START_DATE_TIME+INTERVAL'8'HOUR,'DD-MON-YYYYHH24:MI:SS')": "START_DATE_TIME", \
"TO_CHAR(END_DATE_TIME+INTERVAL'8'HOUR,'DD-MON-YYYYHH24:MI:SS')": "END_DATE_TIME"}, inplace=True)

# ploting the data
plt.style.use("fivethirtyeight")

# starttime=pd.to_datetime(df.START_DATE_TIME.head())


no_abnormal= df[df.START_DATE_TIME != '01-JAN-0001 08:00:00']

print(no_abnormal.SFC.size, df.SFC.size)


# plt.bar(df.START_DATE_TIME.head(), df.PRE_WEIGHT.head(), color="#444444", label="Tests")

# plt.tight_layout()

# plt.show()
