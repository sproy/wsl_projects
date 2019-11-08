import csv
import numpy as np
import pandas as pd
from collections import Counter
from matplotlib import pyplot as plt

plt.style.use("fivethirtyeight")

data = pd.read_csv('./MMDC/scrap_by_reason_code.csv')
su = data[data['Line'] == 'L1_SA_SU']
mu = data[data['Line'] == 'L2_SA_MU']

plc_station_id = su['PLC_STATION_ID'].unique()

fig, axs = plt.subplots(2, 1)
a0 = axs[0].bar(su['REASON_CODE'], su['COUNT'])
a1 = axs[1].bar(su['PLC_STATION_ID'], su['COUNT'])


plt.title("Scrapping distribution")
plt.ylabel("Reason code")
plt.xlabel("PLC station id")

plt.tight_layout()

plt.show()
