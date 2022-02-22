""" Helper functions for scraping data from nba website and building databases """

def toListOfDicts(dataframe, season='2021-22'):
    """ Takes data frame and season (default is 2021-22, same as nbaScraper.py) turns it into a list of dicts"""
    dictList = []
    for row in range(len(dataframe.iloc[:, 0])):
        player_dict = {'season_id': season}
        for k in dataframe.keys():
            player_dict[k.lower()] = dataframe.iloc[row][k]  # db keys are all lowercase

        dictList.append(player_dict)

    return dictList

def find_offline(name=None, player_id=None):
    """
    Finds player name given id or player_id given name
    """
    with open('nba_players_ids.txt', 'r') as file:
        player_data = file.readlines()
        file.close()

    if name is not None:
        for i in range(2,len(player_data),4): # all eligible lines should be full names
            if name.upper() in player_data[i].upper():  # makes everything uppercase so we don't have to check case
                return int(player_data[i-1].strip('id:'))

    elif player_id is not None:
        # returns player name if player_id is found
        for i in range(1,len(player_data),4): # should only iterate id lines
            if str(player_id) in player_data[i]:
                return player_data[i + 1].strip('full_name:')

