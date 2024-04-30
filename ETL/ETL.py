#Extract Transform Load (ETL)
import glob
import pandas as pd     # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.
from datetime import datetime 
from pathlib import Path


#Set Path
tmpfile    = "dealership_temp.tmp"               # file used to store all extracted data
logfile    = "dealership_logfile.txt"            # all event logs will be stored in this file
targetfile = "dealership_transformed_data.csv"   # file where transformed data is stored


# filelink= " https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip"
localfile_path =r"G:\Projects\Data Eng\Python for Data Engineering Project\datasource"
print('path is: ',localfile_path)

# you can use this file path as needed, for example:
# with open(localfile_path + r'\used_car_prices1.csv', 'r') as file:
#     data = file.read()
#     print(data)


#Question 1: CSV Extract Function

def extract_csvfiles(file_name):
    dataframe = pd.read_csv(file_name)
    return dataframe

# ex=extract_csvfiles(localfile_path + r'\used_car_prices1.csv')
# print(ex)



#Question 2: JSON Extract Function

def extract_json(file_name):
 df=   pd.read_json(file_name, lines=True)
 return df

# jsonfile = extract_json(localfile_path + r'\used_car_prices1.json')
# print(jsonfile)

#Question 3: XML Extract Function

def extract_xml(file_name):
    dataframe = pd.DataFrame(columns=['car_model','year_of_manufacture','price', 'fuel'])
    data=[]
    tree = ET.parse(file_name)
    root = tree.getroot()
    for info in root:
        car_model = info.find('car_model').text
        year_of_manufacture = info.find('year_of_manufacture').text
        price = info.find('price').text
        fuel = info.find('fuel').text

        # dataframe = data.append({'car_model': car_model,
        #                                 'year_of_manufacture': year_of_manufacture,
        #                                 'price': price,
        #                                 'fuel': fuel}, ignore_index=True)

        # Append the extracted data to the list as a tuple
        data.append((car_model, year_of_manufacture, price, fuel))
        # Convert the list of tuples to a DataFrame
        dataframes = pd.DataFrame(data, columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])

    return dataframes 


# xmldata = extract_xml(localfile_path + r'/used_car_prices1.xml')
# print(xmldata)


### Question 4: Extract Function


def extract_All():
    extracted_data=[]
            #extract_data= pd.DataFrame(columns=['car_model','year_of_manufacture','price', 'fuel'])
        
                #process all csv files
    for csvfile in glob.glob( localfile_path + '/*.csv'):
            # extract_data=pd.DataFrame.extract_data.concat(extract_csvfiles(csvfile), ignore_index=True)
            extracted_data.append(extract_csvfiles(csvfile))
            

        #process all json files
        
    for jsonfile in glob.glob(localfile_path +'/*.json'):
            extracted_data.append(extract_json(jsonfile))
            #newjsondata=pd.DataFrame(extract_jsondata,columns=columns)
        
        #process all xml files
    for x in glob.glob(localfile_path +'/*.xml'):
        extract_xmldata = extracted_data.append(extract_xml(x))
    columns= ['car_model', 'year_of_manufacture', 'price', 'fuel']

        # newdata=pd.DataFrame(extracted_data,columns=columns,)

        # Concatenate all DataFrames in the extracted_data list
    concatenated_data = pd.concat(extracted_data,ignore_index=True)
    concatenated_data.columns = columns

    print(concatenated_data) 
    return concatenated_data
    

def load(targetfile,data_to_load):
    # with open('extractdata.csv','w') as writeFile:
    #     writeFile.write(data_to_load)

    data_to_load.to_csv(targetfile, index=False) 

def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y' #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("dealership_logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n') 

all_extracted_data = extract_All()

name ='extractdata.csv'

def ask_permission_and_access(file_path, name):
    # Check if the file exists
    if Path(file_path).exists():
        # Ask for permission
        permission = input(f"Do you want to access {file_path}? (yes/no): ").lower()
        if permission == 'yes' or permission == 'y':
            # Perform file operations
            try:
                # with open( '{name}', 'r') as file:
                #     print(file.read())
                load(targetfile='extractdata.csv',data_to_load=all_extracted_data)
                return True            

            except FileNotFoundError:
                     print("File not found.")
                     return False
            except PermissionError:
                 print("Permission denied.")
                 return False

            except Exception as e:
                print(f"Error accessing file: {e}")
                return False
        else:
            print("Permission denied.")
            return False
    else:
        print("File not found.")
        return False


ask_permission_and_access(localfile_path, name)
