from models.model_PlayerCombinedTotals import *
import sys
sys.path.append('/home/nathan/Documents/pythonPractice/SportsStatsPython')
import nbaScraper
import helpers


def main():
    playerCombinedStats.create_table()

    for season in range(2021, 1995, -1):
        season = str(season) + '-' + str(season + 1)[-2:]

        data = helpers.toListOfDicts(nbaScraper.getStats(season), season)

        with db.atomic():
            query = playerCombinedStats.insert_many(data)
        query.execute()
        print(str(season) + ' Completed')


if __name__ == '__main__':
    main()
