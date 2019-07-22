
# coding: utf-8

# In[1]:


import pandas as pd
import sys, os
import json


# In[2]:


# define our pipeline using a JSON file
pipeline_json = """
    [
    {"function":"count_letter", "source":"word_1", "letter":"t", "target":"t_count_1"},
    {"function":"count_letter", "source":"word_2", "letter":"e", "target":"e_count_2"},
    {"function":"sum_columns", "sources":["t_count_1", "e_count_2"], "target":"t1_e2_total"},
    {"function":"rename_column", "source":"t1_e2_total", "target":"letter_count"},
    {"function":"drop_columns", "columns_to_drop":["t_count_1", "e_count_2"]}
    ]
    """

# convert the string to JSON
pipeline_definition = json.loads(pipeline_json)

# export this definition to a text file to simulate the way we would actually want to import the data
with open('pipeline_file.txt', 'w') as outfile:
    json.dump(pipeline_definition, outfile)

# read from the text file to recover our pipeline in JSON format (note json.load NOT json.loads)
with open("pipeline_file.txt") as pipeline_file:
    pipeline_definition = json.load(pipeline_file)
    
pipeline_definition


# In[3]:


# lets build a dataframe
data = {
    "word_1": ["terrible", "totter", "banana", "thick", "transparent"],
    "word_2": ["excellent", "strident", "oblong", "regular", "three"]
       }

df = pd.DataFrame(data)

# export the dataframe to a csv to allow us to simulate importing data
df.to_csv("df.csv", index = False)

# import the csv to get our dataframe
df = pd.read_csv("df.csv")


# In[4]:


df


# In[5]:


# create the standard functions we want to be able to use

class DataFrameHandler():
    
    def __init__(self):
        # initialise an empty pipeline
        self.function_pipeline = []
    
    def rename_column(self, spec):
        '''function to rename a column'''
        # check if this function curries the requested function
        if spec["function"] != "rename_column":
            # if it was not, then return None
            return None
        # otherwise extract the variables to be curried into this function
        source = spec["source"]
        target = spec["target"]
        # then curry the function which wil carry out the requested action
        def fn(df):
            #curried renaming function
            try:
                df = df.rename(columns={source: target})
            except:
                print("unable to complete rename_column")
            return df
        # return the internal function fn to be used in the pipeline
        return fn
    
    def sum_columns(self, spec):
        '''function to sum a list of columns'''
        if spec["function"] != "sum_columns":
            return None
        sources = spec["sources"]
        target = spec["target"]
        def fn(df):
            #curried sum function
            try:
                df[target] = df[sources].sum(axis=1)
            except:
                print("unable to complete sum_columns")
            return df
        return fn
    
    def count_letter(self, spec):
        '''function to create letter frequency counts'''
        if spec["function"] != "count_letter":
            return None
        source = spec["source"]
        target = spec["target"]
        letter = spec["letter"]
        # curry in a helper function to be used on the dataframe using apply
        def _letter_tally(phrase):
            count = 0
            for l in phrase:
                #print("letter = " + l)
                if l == letter:
                    count += 1
            return count
        def fn(df):
            # curried count letter function
            try:
                df[target] = df[source].apply(_letter_tally)
            except:
                print("unable to complete count_letter")
            return df
        return fn
    
    def drop_columns(self, spec):
        '''function to delete a given list of columns from the dataframe'''
        if spec["function"] != "drop_columns":
            return None
        columns_to_drop = spec["columns_to_drop"]
        def fn(df):
            # curried drop columns function
            try:
                df = df.drop(columns_to_drop, axis = 1)
            except:
                print("unable to complete drop_columns")
            return df
        return fn
        
    def create_pipeline(self, pipeline_definition):
        '''create a pipeline from a definition'''
        # enumerate the available pipeline functions
        available_functions = [self.rename_column, 
                               self.sum_columns, 
                               self.count_letter, 
                               self.drop_columns]
        # step through the spec seeking the correct function for each operation
        for spec in pipeline_definition:
            for func in available_functions:
                if func(spec) is not None:
                    # add the desired functions to the pipeline, currying in the required variables
                    self.function_pipeline.append(func(spec))
                    continue
                    
    def clear_pipeline(self):
        '''removes current pipeline functions'''
        self.function_pipeline = []
                    
    def run_pipeline(self, df):
        '''apply the pipeline functions in order to the dataframe'''
        for func in self.function_pipeline:
            df = func(df)
        return df     


# In[6]:


# create our pipeline object
pipeline_object = DataFrameHandler()
# add the correct functions as per the defintion
pipeline_object.create_pipeline(pipeline_definition)
# run the pipeline on our data
output_df = pipeline_object.run_pipeline(df)
output_df

