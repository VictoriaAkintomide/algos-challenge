from flask import Flask, request, jsonify, make_response
from flask_restplus import reqparse
from flask_cors import CORS
import json
import lib.bandit_functions as bdf
import lib.budgeting as budget
import sys
from flask_swagger import swagger
from datetime import date, datetime

NO_AUTHENTICATION = True

app = Flask(__name__)
CORS(app)
# set this bugger by default.
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route("/api_spec")
def api_spec():
    swag = swagger(app)
    swag['info']['version'] = "1.0"
    swag['info']['title'] = "Bayesian Bandit API"
    return jsonify(swag)


@app.route('/', methods=['POST', 'GET'])
def test():
    """
    Sanity check for flask application
    """

    html = "<h3>Hello world 9000! </h3>"
    return html


def process_input():

    json_list = request.get_data()
    if json_list is None:
        json_list = request.get_json()
        print('json', json_list)
    else:
        try:
            json_list = json.loads(json_list)
        except:
            output = {"success": False, "status": 401,
                      "message": "unable to process input data"}
            return False, output

    return True, json_list


BETA_MATRIX = bdf.BetaMatrix()


@app.route('/count', methods = ['POST', 'GET'])
def count_all_events():
    success, output = process_input()

    if success:
        json_list = output

        data_frame = bdf.json_to_df(json_list)
        new_count = bdf.count_events(data_frame)
    
        output = {"success": True, "count": new_count}

    return json.dumps(output), 200, {'Content-Type': 'text/plain; charset=utf-8'}


@app.route('/update_bandit', methods=['POST', 'GET'])
def update_beta_function_from_json():
    success, output = process_input()

    if success:
        json_list = output

        if isinstance(json_list, list) is False:
            json_list = [json_list]

        for game_beta_info in json_list:

            # assert 'item_id' in game_beta_info, 'item_id must be a key'

            expected_inputs = ['item_id', 'num_engagements','num_impressions','num_clickthroughs', 'num_success', 'num_trials', 'daily_spend', 'revenue']

            # process inputs
            inputs = {}
            inputs['item_id'] = game_beta_info.get('item_id') 
            inputs['date'] = game_beta_info.get('date', date.today())
            inputs['item_group_id'] = game_beta_info.get('item_group_id', None)

            for exp_inp in expected_inputs:
                inputs[exp_inp] = game_beta_info.get(exp_inp, 0)

            BETA_MATRIX.update(inputs)

        output = {"success": True, "message": f"{len(json_list)} records appended"}
    return json.dumps(output), 200, {'Content-Type': 'text/plain; charset=utf-8'}



@app.route('/pull_levers', methods=['POST', 'GET'])
def get_all_items_probabilities():
    """
    Pull the lever on all of the bandits
    ---
    get:
        parameters:
              - name: someParam
                in: formData
                type: integer
                required: true
        responses:
            200:
                description: Jsonified dict of predictions
    """

    _, json_list = process_input()

    items = json_list.get('items', None)
    optimizer = json_list.get('optimizer', None)
    local = json_list.get('local', None)
    # dist = json_list.get('dist', None)

    print('items', items, file=sys.stderr)
    best_items, prob_vals = BETA_MATRIX.draw_all_items(items=items, optimizer=optimizer, local=local) #dist=None

    res = []
    for i, itm in enumerate(best_items):
        res.append({"item": itm, "probability": prob_vals[i]})

    output = {"success": True, "random_draws": res}

    return json.dumps(output), 200, {'Content-Type': 'text/plain; charset=utf-8'}

@app.route('/dump_bandit_data', methods=['POST', 'GET'])
def dump_beta_matrix():
    """
    Return all the current beta-matrix parameter, with their ids/game keys
    ---
    get:
        parameters:
              - name: someParam
                in: formData
                type: integer
                required: true
        responses:
            200:
                description: Jsonified dict of predictions, containing all beta matrix parameters
    """
    _, json_list = process_input()

    item_id_list = json_list.get('item_id_list', [])
    item_group_id_list = json_list.get('item_group_id_list', [])


    bandit_data = BETA_MATRIX.dump_data(item_id_list = item_id_list, item_group_id_list = item_group_id_list)
    output = {"success": True, "bandit_data": bandit_data}

    return json.dumps(output), 200, {'Content-Type': 'text/plain; charset=utf-8'}


def create_app():
    """ Constructor
    Returns
    -------
    app : flask app
    """
    return app

if __name__ == "__main__":
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = 80

    # , use_reloader=False) # remember to set debug to False
    app.run(host='0.0.0.0', port=port, debug=NO_AUTHENTICATION)
