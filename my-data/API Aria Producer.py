#!/usr/bin/env python
# coding: utf-8

# ## Import

# In[1]:


#get_ipython().system('pip install sodapy')
#get_ipython().system('pip install kafka-python')


# In[1]:


import json
from sodapy import Socrata
from datetime import datetime, timedelta
from dateutil.parser import parse


# ## Functions


def change_format(date, start_format = '%Y-%m-%dT%H:%M:%S.000'):
    '''
    Funzione per modificare il formato di una data. 
    Prende in input la data da modificare ed il suo formato di partenza.
    '''
    return(datetime.strptime(date,start_format).strftime("%d/%m/%Y %H:%M:%S"))
    


def trasf(item,start_format = '%Y-%m-%dT%H:%M:%S.000'):
    '''
    Funzione per adattare il formato dei dati acquisiti tramite API al formato degli altri dati forniti invece in un file.
    Per prima cosa adatta il formato della data e successivamente i nomi degli attributi secondo lo schema in D.
    ''' 
    
    item['data'] = change_format(item['data'], start_format)
    
    D = {'idsensore': 'IdSensore', 'data': 'Data','valore': 'Valore','stato': 'Stato', 'idoperatore': 'idOperatore'}

    for old,new in D.items():
        item[new] = item.pop(old)
    
    return (item)


def time_slice (time,delta=1):
# prende in input un tempo str e restituisce il tempo aumentato di delta
    time = datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.000') + timedelta(hours=delta)
    return datetime.strftime(time,'%Y-%m-%dT%H:%M:%S.000')


# # API request

# In[5]:


client = Socrata("www.dati.lombardia.it",
                 "OgNGi2gJVq7zGzRRdpCPmK3HM",
                  username="42dr396@gmail.com",
                  password="qLGKdDrdjk.3.SG")


# In[6]:


# meteo query temporary
base_query = """
select *
where
    data >=  '{}'
    and data < '{}'
    and idoperatore = '4'
    order by data
"""


# In[7]:


# query per selezionare un lasso di tempo
base_query = """
select *
where
    data >=  '{}'
    and data < '{}'
    order by data
"""


# In[8]:


from kafka import KafkaProducer
import json
import time
# We create a KafraProducer and we pass a lambda function as value serializer.
# This means the message we pass as value of the send method will be converted to JSON
# using the lambda function.
producer = KafkaProducer(
  bootstrap_servers=["kafka:9092"],
  value_serializer=lambda v: json.dumps(v).encode("utf-8"))


# Si parte da *start* e si arriva ad *end*. *time* è una variabile ausiliaria che ad ogni ciclo è il punto di partenza, con *stop* punto di arrivo.
# 
# 
# 


start = '2020-01-01T00:00:00.000' # primo Gennaio
end = '2020-08-01T00:00:00.000'   # primo luglio?

time = start # time variabile ausiliaria
stop = time_slice(time)
query=base_query.format(time,stop)

while(datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.000') < datetime.strptime(end, '%Y-%m-%dT%H:%M:%S.000')):
    #print('iterazione numero: {}'.format(counter))

    results = client.get("nicp-bhqi",query=query) # aria
    #results = client.get("647i-nhxk",query=query)  # meteo 647i-nhxk
    ###### KAFKA
    for item in results:
        producer.send(topic = 'aria',value=trasf(item))
        #print(item)
    ###### KAFKA
    
    # aggiorno time
    time = stop
    stop = time_slice(time)
    # aggiorno la query
    query=base_query.format(time,stop)
    print(time)
    #print(time,stop)






