import requests
import sys
import re
from lxml import html
from bs4 import BeautifulSoup
import pandas as pd
from multiprocessing import Pool
from pyspark.sql.types import *
from multiprocessing.pool import ThreadPool

url="https://raw.githubusercontent.com/Pradeep39/cricket_analytics/master/utilities/cricket_data_wrangling.py"
sc.addPyFile(url)
exec(requests.get(url).text)

batting_dfs=list()
bowling_dfs=list()

if __name__ == '__main__':
    pool=ThreadPool()
    #fetch worldcup squad list
    r = requests.get("http://www.espncricinfo.com/ci/content/squad/index.html?object=1144415")
    soup = BeautifulSoup(r.content, "html.parser")
    for ul in soup.findAll("ul", class_="squads_list"):
     for a_team in ul.findAll("a", href=True):
      team_squad_df = getCWCTeamData(a_team.text,\
                  "http://www.espncricinfo.com"+a_team['href'])

      for index, row in team_squad_df.iterrows():
        try:
            def getBatsmanCallback(resultDF):
                if not resultDF.empty:
                    batting_dfs.append(resultDF)
            pool.apply_async(getPlayerData, args = (row,"batting", ),\
                                callback = getBatsmanCallback)
       
            def getBowlerCallback(resultDF):
                if not resultDF.empty:
                    bowling_dfs.append(resultDF)
            pool.apply_async(getPlayerData, args = (row, "bowling", ),\
                                callback = getBowlerCallback)

        except Exception as ex:
          print("Exception in Main:"+str(ex))
          pass
    pool.close()
    pool.join()
    from pyspark.sql.types import *
from pyspark.sql.functions import *

batting_df = pd.DataFrame({})
bowling_df = pd.DataFrame({})

for df in batting_dfs:
    batting_df=batting_df.append(df)

for df in bowling_dfs:
    bowling_df=bowling_df.append(df)

batting_schema = StructType([StructField("Runs", StringType(), True),StructField("Mins", StringType(), True),StructField("BF", StringType(), True),StructField("4s", StringType(), True),StructField("6s", StringType(), True),StructField("SR", StringType(), True),StructField("Pos", StringType(), True),StructField("Dismissal", StringType(), True),StructField("Inns", StringType(), True),StructField("Opposition", StringType(), True),StructField("Ground", StringType(), True),StructField("Start Date", StringType(), True),StructField("Country", StringType(), True),StructField("PlayerID", StringType(), True),StructField("PlayerName", StringType(), True),StructField("RecordType", StringType(), True),StructField("PlayerImg", StringType(), True),StructField("PlayerMainRole", StringType(), True),StructField("Age", StringType(), True),StructField("Batting", StringType(), True),StructField("Bowling", StringType(), True),StructField("PlayerRole", StringType(), True)])

batting_spark_df=sqlContext.createDataFrame(batting_df,schema=batting_schema)

bowling_schema = StructType([ StructField("Overs", StringType(), True),StructField("Mdns", StringType(), True),StructField("Runs", StringType(), True),StructField("Wkts", StringType(), True),StructField("Econ", StringType(), True),StructField("Pos", StringType(), True),StructField("Inns", StringType(), True),StructField("Opposition", StringType(), True),StructField("Ground", StringType(), True),StructField("Start Date", StringType(), True),StructField("Country", StringType(), True),StructField("PlayerID", StringType(), True),StructField("PlayerName", StringType(), True),StructField("RecordType", StringType(), True),StructField("PlayerImg", StringType(), True),StructField("PlayerMainRole", StringType(), True),StructField("Age", StringType(), True),StructField("Batting", StringType(), True),StructField("Bowling", StringType(), True),StructField("PlayerRole", StringType(), True)])

bowling_spark_df=sqlContext.createDataFrame(bowling_df,schema=bowling_schema)

#distributed data cleansing operations to prepare data for analysis using matplotlib
clean_batting_spark_df = batting_spark_df\
     .filter("Runs!= 'DNB' AND Runs!='sub' AND Runs!='absent' AND Runs!='TDNB'")\
     .withColumn('Runs', regexp_replace('Runs', '[*]', ''))\
     .withColumn('Mins', regexp_replace('Mins', '[-]', '0'))

clean_bowling_spark_df = bowling_spark_df.filter(\
                          "Overs!= 'DNB' AND Overs!='TDNB'")
matplotlib.pyplotasplt
import seaborn as sns
from pylab import rcParams
from skimage import io

selectedPlayerProfile=253802

# Set figure size
rcParams['figure.figsize'] = 8,4

if selectedCountry==selectedPlayerCountry:
    batsman = clean_batting_spark_df\
             .filter(batting_spark_df.PlayerID == selectedPlayerProfile)\
             .toPandas()

    d = batsman['Dismissal']  
    # Convert to data frame
    df = pd.DataFrame(d)
    df1=df['Dismissal'].groupby(df['Dismissal']).count()
    df2 = pd.DataFrame(df1)
    df2.columns=['Count']
    df3=df2.reset_index(inplace=False)

    image = io.imread(selectedPlayerImg)
    fig, ax = plt.subplots()
    explode = [0] * len(df3['Dismissal'])
    explode[1] = 0.1
    ax.pie(df3['Count'], explode= explode, labels=df3['Dismissal'],autopct='%1.1f%%',shadow=True, startangle=90)

    atitle = selectedPlayerName +  "-Pie chart of dismissals"
    plt.suptitle(atitle, fontsize=24)
    newax = fig.add_axes([0.8, 0.8, 0.2, 0.2], anchor='NE', zorder=-1)
    newax.imshow(image)
    newax.axis('off')
    z.showplot(plt)
    plt.gcf().clear()
    import seaborn as sns
from pylab import rcParams
from skimage import io

selectedPlayerProfile=277912

bowler = clean_bowling_spark_df\
         .filter(clean_bowling_spark_df.PlayerID == selectedBowlerProfile)\
         .toPandas()

if not bowler.empty:
   bowler['Runs']=pd.to_numeric(bowler['Runs'])
   bowler['Wkts']=pd.to_numeric(bowler['Wkts'])
   # Set figure size
   rcParams['figure.figsize'] = 8,4
   image = io.imread(selectedBowlerImg)
   fig, ax  = plt.subplots()
   atitle = selectedBowlerName + "- Wickets vs Runs conceded"
   ax = sns.boxplot(x='Wkts', y='Runs', data=bowler)
   plt.title(atitle,fontsize=18)
   plt.xlabel('Wickets')
   newax = fig.add_axes([0.8, 0.8, 0.2, 0.2], anchor='NE', zorder=-1)
   newax.imshow(image)
   newax.axis('off')
   z.showplot(plt)
   plt.gcf().clear()
   ########Virat Kohli world cup remaining group stage match runs predictions#######
#Bangladesh vs India on July 2nd: 67
#Srilanka vs India on July 6th: 67
#India vs Pakistan on July 9th: 67
#India vs Australia on July 14th: 66
##################################################################
   idx=pd.date_range(start=virat_df.index.min(), \
              end=virat_df.index.max(), freq='D')

virat_df = pd.DataFrame(data=virat_df,index=idx, columns=['runs'])

virat_df = virat_df.assign(RunsInterpolated=df.interpolate(method='time'))
decomposition = seasonal_decompose(\
               virat_df.loc['2017-01-01':df.index.max()].RunsInterpolated)
decomposition.plot()
z.showplot(plt)
import matplotlib
from statsmodels.tsa.holtwinters import ExponentialSmoothing
rcParams['figure.figsize'] = 8,6 
series=df.RunsInterpolated
series[series==0]=1
train_size=int(len(series)*0.8)
train_size
train=series[0:train_size]
test=series[train_size+1:]
y_hat = test.copy()
fit = ExponentialSmoothing( train ,seasonal_periods=730,\
                             trend=None, seasonal='add',).fit()
y_hat['Holt_Winter_Add']= fit.forecast(len(test))
plt.plot(test, label='Test')
plt.plot(y_hat['Holt_Winter_Add'], label='Holt_Winter_Add')
plt.legend(loc='best')
z.showplot(plt)
plt.gcf().clear()
from sklearn.metrics import mean_squared_error
from math import sqrt
rms_add = sqrt(mean_squared_error(test, y_hat.Holt_Winter_Add))
print('RMSE for Holts Winter Additive Model:%f'%rms_add)

#####Output#####
# RMSE for Holts Winter Additive Model:40.490023
y_hat['Holt_Winter_Add']= fit.forecast(len(test)+180)
print('########Virat Kohli world cup match by match runs prediction 1 (Holts Winter Additive)#######')
print('########Virat Kohli world cup match by match runs predictions#######')
print('England vs India on June 30th: %d' \
                  %int(y_hat['Holt_Winter_Add'].loc['2019-06-30']))
print('Bangladesh vs India on July 2nd: %d' \
                  %int(y_hat['Holt_Winter_Add'].loc['2019-07-02']))
print('Srilanka vs India on July 6h: %d' \
                  %int(y_hat['Holt_Winter_Add'].loc['2019-07-06']))
print('###############################################################')

########Virat Kohli world cup match by match runs predictions#######
#Bangladesh vs India on July 2nd: 67
#Srilanka vs India on July 6th: 67
#India vs Pakistan on July 9th: 67
#India vs Australia on July 14th: 66
##################################################################

##################################################################
