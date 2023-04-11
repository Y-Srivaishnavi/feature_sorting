# read dataset as csv file
# convert it to pandas dataframe
# get features
# extract url-wise features (~80 features) and load them onto pandas dataframe (nearly 541k urls) -> function better
#   can't use a for-loop for every url, just note down the points where there's a slash and divide the url
#   don't take in features that aren't needed
#   appending row-wise is expensive, how about creating a dict(better)/numpy array and converting it into a dataframe?
# create a dataframe for new set
# save new dataset as kaggle_dataset

import pandas as pd
import re

def parse_url(url):
    # Regular expression pattern to match the URL components
    pattern = r'^(http[s]?://)?(www\.)?([\w\-\.]+)/([\w\-\.]+)([\?].*)?$'

    # Extracting domain, directory, file, and parameters using regex groups
    match = re.match(pattern, url)
    if match:
        domain = match.group(3)
        directory = match.group(4)
        file = ''
        parameters = ''

        # Checking if file and parameters exist in the URL
        if '.' in directory:
            file = directory
            directory = ''
        if match.group(5):
            parameters = match.group(5)

        if re.search(r'[\w\-.]+@[\w\-.]+\.\w+', domain) or re.search(r'[\w\-.]+@[\w\-.]+\.\w+', directory) or re.search(r'[\w\-.]+@[\w\-.]+\.\w+', file) or re.search(r'[\w\-.]+@[\w\-.]+\.\w+', parameters):
            email = True
        else:
            email = False

        return domain, directory, file, parameters, email
    else:
        print('Invalid URL')
        return None
    
features_list = []
with open('features.csv') as feat_file:
    for line in feat_file:
        features_list.append(line.rstrip())

new_dataset = {}

signs_dict = {"dot":".", 
            "hyphen":"-", 
            "underline": "_", 
            "slash":"/", 
            "questionmark": "?", 
            "equal":"=", 
            "at": "@", 
            "and": "&", 
            "exclamation": "!", 
            "space": ",", 
            "tilde": "~",
            "comma": ",", 
            "plus": "+", 
            "asterisk": "∗", 
            "hashtag": "#", 
            "dollar": "$", 
            "percent": "%"}

with pd.read_csv('/home/srivaishnavi/Downloads/phishing_site_urls.csv', chunksize=10) as website_dataset:
    for chunk in website_dataset:
        for line in chunk.iterrows():               
            index, data = tuple(line)
            url, outcome = tuple(data)

            new_dataset[index] = {}
            try:
                domain, directory, file, parameters, email_in_url = parse_url(url)
            except:
                continue

            new_dataset[index]['url_length'] = len(url)
            new_dataset[index]['directory_length'] = len(directory)
            new_dataset[index]['file_length'] = len(file)
            new_dataset[index]['params_length'] = len(parameters)
            new_dataset[index]['email_in_url'] = int(email_in_url)

            for sign_name, sign in signs_dict.items():
                
                if f'qty_{sign_name}_url' in features_list:
                    new_dataset[index][f'qty_{sign_name}_url'] = url.count(sign)

                if f'qty_{sign_name}_domain' in features_list:
                    new_dataset[index][f'qty_{sign_name}_domain'] = domain.count(sign)

                if f'qty_{sign_name}_directory' in features_list:
                    new_dataset[index][f'qty_{sign_name}_directory'] = directory.count(sign)

                if f'qty_{sign_name}_file' in features_list:
                    new_dataset[index][f'qty_{sign_name}_file'] = file.count(sign)

                if f'qty_{sign_name}_parameters' in features_list:
                    new_dataset[index][f'qty_{sign_name}_parameters'] = parameters.count(sign)

new_dataframe = pd.DataFrame(new_dataset)

new_dataframe.to_csv('dataset_kaggle.csv')