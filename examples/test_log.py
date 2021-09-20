from ingester3 import log
import pandas as pd

@log.log_ingester()
def df_change_nada(df,scalar=1):
    return df


@log.log_ingester()
def divide(num1, num2):
    return num1 / num2

divide(num1=10,num2=2)

data_frame = pd.DataFrame({'id':range(1,1000),'val':range(2,1001)})
df_change_nada(df=data_frame)