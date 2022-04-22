# Model input
#  {
#    "data": {
#      "colnames": ["id"],
#      "coltypes": ["STRING"],
#      "rows": [
#        ["1"],
#        ["7"]
#      ]
#    }
#  }

import cdsw, time, os, random, json
import numpy as np
import pandas as pd
from cmlbootstrap import CMLBootstrap
#import seaborn as sns
import copy

def model_status_dv(args):
  rows = args.get("data").get("rows")
  result = {
    "colnames": ['status', 'replica_status'],
    "coltypes": ['STRING', 'INT']
  }
  
  outRows = []
  for row in rows:
    inputs = row
#    inputs[0] = int(inputs[0])

    # Get the various Model CRN details
    HOST = os.getenv("CDSW_API_URL").split(":")[0] + "://" + os.getenv("CDSW_DOMAIN")
    USERNAME = os.getenv("CDSW_PROJECT_URL").split("/")[6]
    API_KEY = os.getenv("CDSW_API_KEY")
    PROJECT_NAME = os.getenv("CDSW_PROJECT")

    cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

    # Get deployed cancel model details using cmlbootstrapAPI

    model_status = cml.get_model(
      {
          "id": inputs[0],
          "latestModelDeployment": True,
          "latestModelBuild": False,
      }
    )

#    print(model_status)

    model_crn = model_status["latestModelDeployment"]["crn"]
#    print(model_crn)

    model_deploy_status = model_status["latestModelDeployment"]["status"]
#    print(model_deploy_status)

    model_replica_status = model_status["latestModelDeployment"]["replicationStatus"]["numReplicasAvailable"]
#    print(model_replica_status)

    # Append rows to result set
    response = [str(model_deploy_status), int(model_replica_status)]
    outRows.append(response)

  result['rows'] = outRows

  return {
    "version": "1.0",
    "data": result
  }


############################
#. Uncomment the following to test the results
############################

#def main():
# use the following for multiple models
#  inData = {
#    "data": {
#      "colnames": ["id"],
#      "coltypes": ["STRING"],
#      "rows": [
#        ["1",],
#        ["2",]
#      ]
#    }
#  }
# use the following for only 1 model
#  inData = {
#    "data": {
#      "colnames": ["id"],
#      "coltypes": ["STRING"],
#      "rows": [
#        ["1",]
#      ]
#    }
#  }

#  print(model_status_dv(inData))

  
#if __name__ == "__main__":
#    main()

############################
