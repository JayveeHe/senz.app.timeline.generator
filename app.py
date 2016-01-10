# -*- encoding:utf-8 -*-
# from collections import Counter
# import logging
import os
import sys
# import logentries

# import logentries

# from ml_utils.crfsuite_utils import print_state_features
# from common_utils import senzdata2crfdata
# from config.MyExceptions import MsgException

__author__ = 'Jayvee'

from flask import Flask, request
import json

project_path = os.path.dirname(__file__)
sys.path.append(project_path)

app = Flask(__name__)


# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# lh = logentries.LogentriesHandler(token_config.LOGENTRIES_TOKEN)
# fm = logging.Formatter('%(asctime)s : %(levelname)s, %(message)s',
#                        '%a %b %d %H:%M:%S %Y')
# lh.setFormatter(fm)
# logger.addHandler(lh)



@app.route('/status', methods=['GET'])
def check_status():
    return 'The %s server is running' % 'senz.app.timeline.generator'



if __name__ == "__main__":
    app.debug = False
    app.run(host='0.0.0.0', port=3333)
