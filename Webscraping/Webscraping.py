#Webscraping


import traceback
from bs4 import BeautifulSoup
import json
import requests
import pandas as pd
import os




def fetchdata(url, output_file):

    try:
        response = requests.get(url)
        if response.status_code!=200:
            print('Failed to fetch webpage')
        
        if response.status_code==200:
            html_content= response.text

            # parse html using beautiful soap
            soup= BeautifulSoup(html_content,'html.parser')

            table= soup.find('table',class_='wikitable')
            if table:
                data=[]
                headers = None  # Initialize headers to a default value
                header=[]
                for row in table.find_all('tr'):
                    cells =row.find_all(['th','td'])
                    if cells:
                      if not headers:
                        headers = [cell.text.strip() for cell in cells]
                      else:
                            data.append({headers[i]: cell.text.strip() for i, cell in enumerate(cells)})
                 # Write data to JSON file
                with open(output_file, 'w') as json_file:
                 json.dump(data, json_file, indent=4)
                 print(f"Data scraped and stored in '{output_file}'.")
            
            else:
              print("Table not found on the webpage.")

    
    except Exception as e:
          # Convert the exception to a string
        exception_str = traceback.format_exc()
        print('Error:\n {exception_str}')



        
url="https://web.archive.org/web/20200318083015/https://en.wikipedia.org/wiki/List_of_largest_banks"
output_file = "largest_banks.json"

fetchdata(url=url,output_file=output_file)

# Get the current working directory
current_directory = os.getcwd()

# Combine the current directory with the filename
file_path = os.path.join(current_directory, 'largest_banks.json')

# Print the file path
print("The 'largest_banks.json' file is located at:", file_path)