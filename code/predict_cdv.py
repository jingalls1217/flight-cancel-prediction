import numpy as np
import pandas as pd
import cdsw
import json
from joblib import dump, load

# args = {"feature" : "US,DCA,BOS,1,16"}

ct = load("models/ct.joblib")
pipe = load("models/pipe.joblib")

def predict_dv(args):
  rows = args.get("data").get("rows")
  result = {
    "colnames": ['prediction', 'proba'],
    "coltypes": ['INT', 'STRING']
  }
  
  outRows = []
  for row in rows:
    inputs = row
    inputs[3] = int(inputs[3])
    inputs[4] = int(inputs[4])
    
    input_cols = [
        "OP_CARRIER",
        "ORIGIN",
        "DEST",
        "WEEK",
        "HOUR",
    ]
    input_df = pd.DataFrame([inputs], columns=input_cols)

    input_transformed = ct.transform(input_df)

    probas = pipe.predict_proba(input_transformed)
    prediction = np.argmax(probas)
    proba = round(probas[0][prediction], 2)
    
    response = [int(prediction), str(proba)]
    outRows.append(response)

  result['rows'] = outRows

  return {
    "version": "1.0",
    "data": result
  }
