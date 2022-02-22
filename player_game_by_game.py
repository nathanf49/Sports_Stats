from models.model_PlayerGameLog import *
from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.library.parameters import SeasonAll
import time

player_dictionaries = players.get_players()

def main():
    start_time = time.time()

    for player in player_dictionaries:
        player_start_time = time.time()
        print('Now working on ' + player['full_name'])
        try:
            gameLog = playergamelog.PlayerGameLog(player_id=player['id'], season=SeasonAll.all).get_normalized_dict()
        except: # error caused by timeout, try again
            gameLog = playergamelog.PlayerGameLog(player_id=player['id'], season=SeasonAll.all).get_normalized_dict()
        ### test code to add player names to db
        for row in gameLog['PlayerGameLog']:
            row['Player_Name'] = player['full_name']
        ##############################################33
        with db.atomic():
            query = player_game_log.insert_many(gameLog['PlayerGameLog'])
        query.execute()
        print("Player writing time: " + str(time.time() - player_start_time))

    print("Total time: " + str(time.time() - start_time))

def season_averages():
    pass

if __name__ == '__main__':
    init_database()
    main()

""""
Querying:
player_game_log.get(query) # gets first row matching query
player_game_log.select().dicts() # gets all rows in db in dict form
player_game_log.select().where(query).dicts # gets all rows satisfying query in dict form

"""