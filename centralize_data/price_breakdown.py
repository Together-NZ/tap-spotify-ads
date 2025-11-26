import yaml
from google.cloud import bigquery
import calendar
import datetime
import pandas as pd
START_YEAR=datetime.datetime.now().year # default always start from today's date
START_MONTH=datetime.datetime.now().month
START_DAY=datetime.datetime.now().day
START_YEAR=2025
START_MONTH=1
END_MONTH=11
START_DAY=1
today=datetime.date.today()
LAST_DAY = calendar.monthrange(today.year, today.month)[1]

def get_query(advertiser):
    query_list=[]
    if advertiser.get('brands'):
        suffix=advertiser.get('brands')
        for brand in suffix:
            suffix=brand.get('id')
            query=construct_query(project=advertiser.get('project_id'),dataset='dash_table',table='dash_union',suffix=suffix)
            query_list.append(query)
    else:
        query=construct_query(project=advertiser.get('project_id'),dataset='dash_table',table='dash_union')
        query_list.append(query)
    return query_list

def construct_query(project: str,dataset: str,table: str,suffix: str=None ):
    if suffix:
        dataset=f"{dataset}__{suffix}"
        table=f"{table}__{suffix}"
    query=f"""
    SELECT SUM(media_cost) AS media_cost,publisher FROM `{project}.{dataset}.{table}` 
    WHERE date between '{START_YEAR}-{START_MONTH}-{START_DAY}' and '{START_YEAR}-{END_MONTH}-{LAST_DAY}'
    GROUP BY publisher
    """
    return query


__name__ == "__main__"
with open('data.yml', 'r') as f:
    data = yaml.safe_load(f)

advertiser_data=data.get("advertisers",{})

pre=None
cur=None
for advertiser in advertiser_data:

    client=bigquery.Client(project=advertiser.get('project_id'))

    query_list = get_query(advertiser)
    
    if len(query_list)>1:
        df_list = []
        for query in query_list:
            try:
                df=client.query(query).to_dataframe()
            except Exception as e:
                continue
            df_list.append(df)
        df=pd.concat(df_list,axis=0)
        df["advertiser"]=advertiser.get('name')
        df=df.pivot_table(index='advertiser',columns='publisher',values='media_cost',aggfunc='sum').reset_index()
    else:
        try:
            df=client.query(query_list[0]).to_dataframe()
        except Exception as e:
            continue
        df["advertiser"]=advertiser.get('name')
        df=df.pivot_table(index='advertiser',columns='publisher',values='media_cost',aggfunc='sum').reset_index() 
    cur=df
    if pre is None:
        pre=cur
        cur=None
    else:
        merge=pd.merge(pre,cur,how='outer')
        pre=merge
pre.to_csv(f"publisher_platform.csv")
    



    
