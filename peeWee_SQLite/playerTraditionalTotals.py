from models.model_PlayerTraditionalTotalStats import *
import sys
import pandas as pd
sys.path.append("/home/nathan/Documents/pythonPractice/SportsStatsPython")
import nbaScraper
from helpers import toListOfDicts


def main():
    playerTraditionalTotalStats.create_table()

    for season in range(2022, 1996, -1):  # nba site only goes back to 96-97
        season = str(season)
        season = season + '-' + str(int(season) + 1)[-2:]  # format 2021-22

        data = toListOfDicts(nbaScraper.getStats(season=season, stat_type='Traditional'), season)

        with db.atomic():
            query = playerTraditionalTotalStats.insert_many(data)
        query.execute()
        print(season + ' completed')


if __name__ == '__main__':
    main()
