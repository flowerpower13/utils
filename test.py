import pandas as pd
import numpy as np

# Sample data
data = {
    'firm': ['a', 'a', 'a', 'a', 'a', 
             'b', 'b', 'b', 'b', 'b'],

    'year': [2000, 2001, 2002, 2003, 2004, 
             2000, 2001, 2002, 2003, 2004],
    'enforcement_dummy': [0, 0, 0, 0, 0, 
                          0, 0, 1, 0, 0],
    'state': ["illinois", "alabama", "louisiana", "california", "illinois", 
              "illinois", "illinois", "illinois", "illinois", "illinois", ]
}

df = pd.DataFrame(data)
print(df)

codes, uniques = pd.factorize(df['state'])

print(codes)

array=list(codes)

print(array)