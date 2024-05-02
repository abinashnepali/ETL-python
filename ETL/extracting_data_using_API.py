
import requests
import pandas as pd

# Extracting Data using API

#you can go to link https://www.exchangerate-api.com/  and create free  api key

#Question
## Extract Data Using an API
#Using the data gathered turn it into a `pandas` dataframe. The dataframe should have the Currency as 
#the index and `Rate` as their columns. Make sure to drop unnecessary columns.
#Using the dataframe save it as a CSV names `exchange_rates_1.csv`.


class ExtractingData:
     
     def __init__(self, value):
        self.data = value

    #sample_url = 'https://v6.exchangerate-api.com/v6/YOUR-API-KEY/latest/USD'
    #  base_url='https://v6.exchangerate-api.com/v6/'
     api_key='your API_key here'  
     end_point='latest/USD'

     def getExchangeRate(apikey,endpoint):
        base_url='https://v6.exchangerate-api.com/v6/'
        finalurl=f'{base_url}/{apikey}/{endpoint}'
        print(finalurl)
        response=  requests.get(finalurl)
        data= None
        if response.status_code==400:
            
            
            print('Server Error')
            
        elif response.status_code!=200:
                
                print('Something is wrong')
            
        elif response.status_code==200:
            info= response.json()
            if info:
                    data=info['conversion_rates']                   
                    print ('data in not empty')
                    print(data)
                
            else:
                    print('data is empty')
        else:
            print('Something is wrong')
                
        return data 

   # Function End Here  

     datainfo = getExchangeRate(apikey=api_key,endpoint='latest/USD')

     def convertDataFrame(jsondata):
        df=None
        if jsondata:
             dataframe = pd.DataFrame(jsondata.items(), columns=['Currency','Rate'])
             # Set "Currency" column as index
             dataframe.set_index("Currency", inplace=True)
             
             print (dataframe)

             df =dataframe
            
        
        else:
              print('No data')
        return df

     # Function End Here  
     df = convertDataFrame(datainfo)

     def savefile(dataFrame):
        if dataFrame is not None:
                
                #dataFrame.to_csv('/local_path_name/exchange_rates_1.csv')

                dataFrame.to_csv('exchange_rates_1.csv')
                print("DataFrame saved successfully as exchange_rates_1.csv")

        else:
             print("Error: DataFrame is None, cannot save to CSV")

     savefile(df)      
  



    
        