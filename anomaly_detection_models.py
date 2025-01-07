#%%
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler
import numpy as np
from sklearn.impute import SimpleImputer
# %%
df = pd.read_json('sensor_data.json')
df.head()
# %%
df.describe()
# %%
column= ['temperature','humidity','sound_volume']
for i in column:
    plt.figure(figsize=(8,6))
    sns.histplot(df[i], bins = 50, kde=True)
    plt.title(f'Histogram of {i} distribution of wind turbine')
    plt.show()
# %%
df.isnull().sum()
# %%
sns.pairplot(df)
# %%
sns.heatmap(df.corr(), annot= True)

# %% preprocess data 
def preprocess_data(df):
    df = df.fillna(df.mean())
    scaler = MinMaxScaler()
    numerical_columns = df.columns[df.dtypes != 'object']  # Select numerical columns
    df[numerical_columns] = scaler.fit_transform(df[numerical_columns])
    return df

cleaned_df = preprocess_data(df)
cleaned_df
# %% 

# %%
