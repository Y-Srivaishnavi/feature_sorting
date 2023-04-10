import pandas as pd
import os
import matplotlib.pyplot as plt

# reading dataset
dataset = pd.read_csv('./dataset_full.csv')

# gathering list of all features
column_list = list(dataset.columns)

might_delete = []

# removing outliers
for feature in column_list:
    if feature == column_list[111]:
        break  
    
    if len(dataset[dataset[feature] != -1]) != len(dataset[feature]):

        if len(dataset[dataset[feature] == -1]) > len(dataset[feature])//2:
            might_delete.append(feature)
            continue
        else:
            dataset.loc[dataset[feature] == -1, feature] = pd.Series.mean(dataset[feature])

dataset = dataset.drop(might_delete, axis=1)
horrible_variances = ['length_url','time_domain_expiration']

for features in horrible_variances:
    dataset = dataset.drop(labels=dataset.loc[dataset[features] == max(dataset[features])].index, axis=0)

new_column_list = [feature for feature in column_list if feature not in might_delete]

# number of features = 112, classification into phishing/non-phishing is in the last column
outcome = dataset[new_column_list[-1]]

# calculate feature-wise correlation coefficient 
corr_matrix = pd.DataFrame.corrwith(dataset, outcome)

# create dictionaries for features, map values of correlation to them
major_features = {}
average_features = {}
minor_features = {}
dropped_features = {}

# feature-wise correlation plots
for index, feature in enumerate(new_column_list):
    if feature == new_column_list[-1]: # no need to plot correlation of outcome with itself
        break  
    
    weightage = abs(corr_matrix[index])
    variance = pd.Series.var(dataset[feature])

    if weightage >= 0.7 and variance >= 0.7:
        major_features[feature] = {}
        major_features[feature]['corr'] = corr_matrix[index]
        major_features[feature]['var'] = variance
    
    elif weightage >= 0.4 and variance >= 0.4:
        average_features[feature] = {}
        average_features[feature]['corr'] = corr_matrix[index]
        average_features[feature]['var'] = variance
    
    elif weightage >= 0.1 and variance >= 0.1:
        minor_features[feature] = {}
        minor_features[feature]['corr'] = corr_matrix[index]
        minor_features[feature]['var'] = variance
    else:
        dropped_features[feature] = {}
        dropped_features[feature]['corr'] = corr_matrix[index]
        dropped_features[feature]['var'] = variance
        
# write features into a csv file
with open('./features.csv', 'a') as features_file:

    print('*'*50,file=features_file)

    print('Major Features: ',file=features_file)
    for major in major_features.items():
        print(major, file=features_file)    
    print('\n',file=features_file)

    print('Average Features: ', file=features_file)
    for average in average_features.items():
        print(average, file=features_file)  
    print('\n',file=features_file)

    print('Minor Features: ', file=features_file)
    for minor in minor_features.items():
        print(minor, file=features_file)  
    print('\n',file=features_file)

    print('Dropped Features: ',file=features_file)
    for dropped in dropped_features.items():
        print(dropped, file=features_file)  

