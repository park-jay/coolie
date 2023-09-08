import pandas as pd
import numpy as np
from nltk import word_tokenize
import os
from ast import literal_eval
from nltk.util import ngrams
from nltk.corpus import stopwords
stop=stopwords.words('english')
# import spacy
# nlp=spacy.load('en_core_web_sm')
from tqdm import tqdm
import concurrent.futures

path='/Users/mstudio/Library/CloudStorage/Box-Box/coolie/sent/'
# path='/projects/ischoolichass/ichass/jaihyun2/'
df=pd.read_csv(path+'coolie-lemma.csv', converters={'state':literal_eval, 'context':literal_eval, 'city':literal_eval})


def calculate_reuse(k, p, data):
    no_reuse = []
    pair_index1 = []
    pair_index2 = []
    pair_date1 = []
    pair_date2 = []
    pair_text1 = []
    pair_text2 = []
    pair_state1 = []
    pair_state2 = []

    if (k == p) & (len(set(data['5-gram'][k]).intersection(data['5-gram'][p])) == len(data['5-gram'][k])):
        pass
    elif len(set(data['5-gram'][k]).intersection(data['5-gram'][p])) == 0:
        pass
    elif len(set(data['5-gram'][k]).intersection(data['5-gram'][p])) >= 3:
        no_reuse.append(len(set(data['5-gram'][k]).intersection(data['5-gram'][p])))
        pair_index1.append(k)
        pair_index2.append(p)
        pair_date1.append(data.iloc[k]['date'])
        pair_date2.append(data.iloc[p]['date'])
        pair_text1.append(data.iloc[k]['lemma'])
        pair_text2.append(data.iloc[p]['lemma'])
        pair_state1.append(data.iloc[k]['state'])
        pair_state2.append(data.iloc[p]['state'])

    return no_reuse, pair_index1, pair_index2, pair_date1, pair_date2, pair_text1, pair_text2, pair_state1, pair_state2

def reuse_df(data: pd.DataFrame()):
    no_reuse = []
    pair_index1 = []
    pair_index2 = []
    pair_date1 = []
    pair_date2 = []
    pair_text1 = []
    pair_text2 = []
    pair_state1 = []
    pair_state2 = []

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        total_tasks = data.shape[0] * (data.shape[0] - 1) // 2

        # Use tqdm to create a progress bar
        with tqdm(total=total_tasks, desc="Calculating Reuse") as pbar:
            for k in range(0, data.shape[0]):
                for p in range(k + 1, data.shape[0]):
                    futures.append(executor.submit(calculate_reuse, k, p, data))

            # Collect results from futures
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result[0]:  # Check if there is any data to append
                    no_reuse.extend(result[0])
                    pair_index1.extend(result[1])
                    pair_index2.extend(result[2])
                    pair_date1.extend(result[3])
                    pair_date2.extend(result[4])
                    pair_text1.extend(result[5])
                    pair_text2.extend(result[6])
                    pair_state1.extend(result[7])
                    pair_state2.extend(result[8])
                    pbar.update(1)

    return pd.DataFrame({'no_reuse': no_reuse, 'pair_index1': pair_index1, 'pair_index2': pair_index2,
                         'pair_date1': pair_date1, 'pair_date2': pair_date2,
                         'pair_text1': pair_text1, 'pair_text2': pair_text2,
                         'pair_state1': pair_state1, 'pair_state2': pair_state2})

if __name__ == "__main__":
    # Assuming you have a DataFrame 'df'
    reprintdf = reuse_df(df)
    reprintdf.to_csv(path+'reprintdf.csv', index=False)
