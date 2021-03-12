from scipy.stats import beta
import numpy as np
import pandas as pd
import json
import sys
import os
from datetime import date
from pathlib import Path

this_dir = os.path.dirname(os.path.abspath(__file__)) + '/'
sys.path.append(this_dir)

import db_functions as dbf



def json_to_df(json_data):
    """
    Function that takes a json file/object and converts it into a pandas dataframe. Also resets the indexes incase the incoming raw tracking file has incorrect indices 

    Parameters
    ----------
    file : .json
        Raw tracking data in json format from AWS S3 bucket, downloaded with the specified columns and types of engagements, for the dates when the campaign is running

    Returns
    -------
    data : pandas.DataFrame
        Dataframe with correct indices, ready for messaging
    """

    data_frame = pd.DataFrame(json_data)
    return data_frame


def count_events(df, id_key='line_item_id', total='impression',
                 success='first_dropped',group_key='auction_id', click=None):
    """
    Function which takes in a dataframe, groups the data by user/auction_id, and counts the total number of impressions/first_droppeds for each game key

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe containing raw data with different engagement types, auction_ids, campaign_ids and line_item_ids
        group_key, total, success and id_key metrics can be changed according to use. The defaults are useful in most cases

    Returns
    -------
    count_dict : dictionary
        Dictionary of game keys with their respective count of impressions as number_trials and count of first_dropped as number_successes
        This dictionary is filtered by removing keys with None and empty strings, and removing entries with 0 impressions, before it is returned
    """

    dfg = df.groupby(id_key)
    count_dict = []
    
    # auction_id is the metric
    for name, dfx in dfg:
        
        c_name = list(dfx['campaign_id'])[0]
        dfxg = dfx.groupby('type') 
        nimp = len(dfxg.get_group(total)[group_key].unique())
        neng = len(dfxg.get_group(success)[group_key].unique())
        
        if click == True:
            nclick = len(dfxg.get_group('click-through-event')[group_key].unique())
        else:
            nclick = 0
        
        nsuc = neng + nclick
        count_dict.append({'item_id': name, 'item_group_id': c_name,'num_success':nsuc, 'num_trials':nimp, 'num_clickthroughs': nclick, 'num_engagements': neng, 'num_impressions': nimp})
        
    return count_dict


class BetaMatrix:
    """
    Class for performing classical bayesian bandits where item = creative = ad = game_key = line_item_id = AdGroupId
    """

    def __init__(self, verbose=False, testing=False):
        """
        Initilializing beta_matrix and items it contains
        initial_beta_matrix is of shape [Nitems, 2] and items are all the game keys for this campaign, with [a, b] for each game-key
        """

        self.data_dict = None
        self.kpi_campaign = None
        self.verbose = verbose
        self.testing = testing


        # load from DB memory
        self._update_band()
        
        # load the latest KPIs for each campaign
    def _update_band(self):
        if self.testing==True:
            self.data_dict = count_events(pd.read_json(data))
        else:
            self._load_latest_beta_functions()
        self._load_latest_kpis()
        pass

    def _load_latest_beta_functions(self):
        """
        preloads beta functions from a DB/file somwhere
        connect to DB/file
        load content
        add content using self._add_item
        """

        self.data_dict = dbf.restore_from_db()

        pass

    def _load_latest_kpis(self):
        """
        CONNECT TO A DB and get this information 
        """
        #list of kpis in order highest to lowest [paypoint, kpi1, kpi2] : [10, 3, 1]

        kpi_campaign = {}

        for item_dict in self.data_dict:
            # print(item_dict)
            if item_dict['item_group_id'] not in kpi_campaign:
                kpi_campaign[item_dict['item_group_id']] = {'num_engagements':1} 

        self.kpi_campaign = kpi_campaign

        pass


    def update(self, item_dict):
        """
        Function updates the current beta matrix with new values added using _add_item function, with number of engagements/first_dropped equal to a(number of successes) 
        and number of impressions - number of engagements equal to b(number of failures). 

        Parameters
        ----------
        item : string
            Game/Ad/Line item id/key that needs to be updated
        success : number of engagements/first_dropped
            Number of times users engaged with the creative
        trials : number of impressions
            Total number of times the creative was shown to users


        """

        # converts dictionary to keywords
        dbf.update_insert_database(**item_dict)

        pass

    def dump_data(self, item_id_list = [], item_group_id_list = []):
        return dbf.dump_db(item_id_list= item_id_list, item_group_id_list= item_group_id_list)


    def draw_best_item(self):
        """
        Function uses the randomly drawn distributions from the previous function(rand_draw) to return index of the game/item with the highest probability of success. 

        Parameters
        ----------
        rand_draws : numpy.array
            Random probabilites drawn from each of the items' beta distributions
        items : list of strings
            All game/ad ids

        Returns
        -------
        best game id : string
            ID of the game/item with the highest probability from the drawn beta distributions
        rand_vals : numpy.array
            Array containing the final probabilites of all items. 
            This is returned here because calling the random_draw function randomly draws from beta distributions again, 
            leading to different best game id and probabilties if the two functions are called separately
        """

        items, vals = self.draw_all_items()
        return items[0], float(vals[0])

    def draw_all_items(self,  items=None, trial_key='num_impressions', optimizer=None, local=None, dist=None):
        """
        Function uses the randomly drawn distributions from the previous function(rand_draw) to return index of the game/item with the highest probability of success. 

        Parameters
        ----------
        rand_draws : numpy.array
            Random probabilites drawn from each of the items' beta distributions
        items : list of strings
            All game/ad ids

        Returns
        -------
        all game ids : string array
            ID of the game/item with the highest probability from the drawn beta distributions
        rand_vals : numpy.array
            Array containing the final probabilites of all items. 

        """

        # update_bandit information and KPI
        if local:
            self.data_dict = local
        else:
            self._update_band()

        # draw from all beta functions
        rand_draws = {}
        print(self.data_dict)
        for item_dict in self.data_dict:
#             print(item_dict)
            # get the "target" of this campaign
            c_id = item_dict['item_group_id']
            if self.testing==True:
                pass
            else:
                cost = item_dict['daily_spend']
            succ = self.kpi_campaign[c_id]
            num_sucss = 0
            num_trials = 0
             
            for kpi in succ:
                num_sucss += item_dict[kpi]*succ[kpi]
                
            if num_sucss == 0:
                num_sucss = item_dict['num_engagements']
                if num_sucss == 0:
                    continue

            num_trials += item_dict[trial_key]*np.sum(list(succ.values()))

            rnds = [ np.random.beta(1 + num_sucss, 1 + num_trials - num_sucss) for r in range(100) ]
            rnd = np.array(rnds).mean()
            rand_draws[item_dict['item_id']] = float(rnd)
                
        # remove unrequired game_keys
        item_keys = list(rand_draws.keys())
        if items is not None:
            item_keys = np.array([i for i in item_keys if i in items])

        # get vals of items
        vals = np.array([rand_draws[i] for i in item_keys])
        # sort by vals
        srt_ind = np.argsort(vals)[::-1]

        return np.array(item_keys)[srt_ind], vals[srt_ind]







