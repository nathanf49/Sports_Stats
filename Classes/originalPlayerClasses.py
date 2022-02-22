"""
keeps each game as a game object. Much slower than nbaClasses.py as
"""
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.library.parameters import SeasonAll
import time
from nba_api.stats.endpoints import leaguegamefinder

team_dict = teams.get_teams()
player_dict = players.get_players()


class Player:  # import player information from dictionaries
    def __init__(self, playerName=None):
        while type(playerName) is not str:
            playerName = str(input('Please input the name of a current or past NBA player: '))
        if ' ' in playerName:  # name is a full name
            for player in player_dict:
                if player['full_name'].lower() == playerName.lower():  # avoid case issues by converting all to lower
                    self.id = player['id']
                    self.name = player['full_name']
                    self.isActive = player['is_active']
                    self.seasons = self.getStats()

        else:  # check for player first or last name
            for player in player_dict:
                if player['first_name'].lower() == playerName.lower() or player['last_name'].lower() == playerName.lower():
                    checkPlayer = ''
                    # confirm since we don't have the full name
                    while checkPlayer not in ['yes', 'Yes', 'YES', 'YEs', 'Y', 'y', 'no', 'No', 'NO', 'n', 'N']:
                        checkPlayer = input('Did you mean ' + player['full_name'] + '?')
                    if checkPlayer in ['yes', 'Yes', 'YES', 'YEs', 'Y', 'y']:  # only input player if the answer is yes
                        self.id = player['id']
                        self.name = player['full_name']
                        self.isActive = player['is_active']
                        self.seasons = self.getStats()

        if hasattr(self, 'id') is False:  # player was not found
            raise Exception(str(playerName) + " could not be found. You may want to check your spelling.")

        # get career stats
        self.gamesPlayed = self.getGames()
        self.PTS = self.getPTS()
        self.PPG = self.getPPG()
        self.REB = self.getREB()
        self.RPG = self.getRPG()
        self.AST = self.getAST()
        self.APG = self.getAPG()
        self.activeSeasons = self.getActiveSeasons()

    def getGames(self, season=None, team=None):  # gets number of games played
        # allows user to get games from a specific season
        validSeasons = self.seasonInputChecker(season)

        # allows user to get games against a specific team or just total games
        totalGames = 0
        if team is None:
            for year in validSeasons:
                totalGames += len(year.games)
        else:
            team = getTeamAbbreviation(team)
            for year in validSeasons:
                totalGames += year.getNumberOfGames(team)
        return totalGames

    def getPTS(self, season=None, team=None):  # get points scored
        # allows user to get games from a specific season
        validSeasons = self.seasonInputChecker(season)

        totalPoints = 0
        if team is None:
            for year in validSeasons:
                totalPoints += year.PTS
        else:  # go season by season to get stats against a specific team
            team = getTeamAbbreviation(team)
            for year in validSeasons:
                totalPoints += year.getSeasonPTS(team)
        return totalPoints

    def getPPG(self, season=None, team=None):  # get average points scored per game
        # getPTS and getGames only gets season and team inputs that aren't None, so any mix will arrive correctly
        if team is not None:
            team = getTeamAbbreviation(team)
        return round(self.getPTS(season=season, team=team) / self.getGames(season=season, team=team), 2)

    def getREB(self, season=None, team=None):  # get total rebounds
        # allows user to get games from a specific season
        validSeasons = self.seasonInputChecker(season)

        totalREB = 0
        if team is None:
            for year in validSeasons:
                totalREB += year.REB
        else:  # goes season by season to find stats against a specific team
            team = getTeamAbbreviation(team)
            for year in validSeasons:
                totalREB += year.getSeasonREB(team)
        return totalREB

    def getRPG(self, season=None, team=None):  # get average rebounds per game
        if team is not None:
            team = getTeamAbbreviation(team)
        return round(self.getREB(season=season, team=team) / self.getGames(season=season, team=team), 2)

    def getAST(self, season=None, team=None):  # get total assists
        # allows user to get games from a specific season
        validSeasons = self.seasonInputChecker(season)

        totalAST = 0
        if team is None:
            for year in validSeasons:
                totalAST += year.AST
        else:  # goes season by season to find stats against a specific team
            team = getTeamAbbreviation(team)
            for year in validSeasons:
                totalAST += year.getSeasonAST(team)
        return totalAST

    def getAPG(self, season=None, team=None):  # get average assists per game
        if team is not None:
            team = getTeamAbbreviation(team)
        return round(self.getAST(season=season, team=team) / self.getGames(season=season, team=team), 2)

    def getActiveSeasons(self):
        activeSeasons = []
        for year in self.seasons:
            activeSeasons.append(int(str(year.games[0].seasonID)[-4:]))
        return activeSeasons

    def getStats(self, season=None):
        if season is not None:  # updating a season, not the whole career
            while len(str(season)) != 4:
                season = int(input('Please enter the season you want to update as a 4 digit year'))
            gamestats = playergamelog.PlayerGameLog(player_id=self.id, season=season).get_dict()
            'need to finish updates after importing career stats'
            seasonGames = []
            for game in gamestats['resultSets'][0]['rowSet']:  # saves all player stats from individual games
                seasonGames.append(
                    GameStats(game[24], game[18], game[19], game[0], self.id, game[2],
                              game[3], game[4], game[5], game[6], game[7],
                              game[8], game[9], game[10], game[11], game[12],
                              game[13], game[14], game[15], game[16], game[17], game[20],
                              game[21], game[22], game[23], game[25]))
            return Season(str(season), seasonGames)

        else:
            # gets career dataframes from nba_api module
            careerStats = playergamelog.PlayerGameLog(player_id=self.id, season=SeasonAll.all).get_data_frames()

            # makes a list of all years the player has played in the nba
            activeYears = []
            for year in careerStats[0]['SEASON_ID']:
                if str(year)[-4:] not in activeYears:
                    activeYears.append(str(year)[-4:])

            # makes season stats 1 year at a time
            career = []
            for year in activeYears:
                career.append(self.getStats(year))
            # after making each year as an individual season, cast as a Career
            return career

    def seasonInputChecker(self, season):  # only return season inputs are years that player was active
        if season is None:  # if the year is not input all seasons are valid
            validSeasons = self.seasons
        elif type(season) is int:
            if season not in self.activeSeasons:
                raise ValueError('Player was not active during this season')
            else:
                validSeasons = [self.getStats(season)]
        elif type(season) is list:
            validSeasons = []
            for year in season:
                if year not in self.activeSeasons:
                    raise ValueError('Player was not active during this season')
                else:
                    validSeasons.append(self.getStats(year))
        else:
            raise TypeError('Seasons must be input as an integer or a list of integers')
        return validSeasons

    '''
    These functions are for a Player and are not really used anymore but could be helpful in the future
    
    def addSeason(self, season):  # allows a season to be added to a players career
        if type(season) is Season:
            self.seasons.append(season)
        elif type(season) is list:
            for current in season:
                if type(current) is Season:
                    self.seasons.append(current)
                else:
                    raise TypeError('Only seasons can be added to a career')
        elif season is not None:
            raise TypeError('Only seasons can be added to a career')
        
    def updateStats(self):
        self.gamesPlayed = self.getGames()
        self.PTS = self.getPTS()
        self.PPG = self.getPPG()
        self.REB = self.getREB()
        self.RPG = self.getRPG()
        self.AST = self.getAST()
        self.APG = self.getAPG()
        self.activeSeasons = self.getActiveSeasons()
    '''

    def __str__(self):
        return 'PPG: ' + str(self.PPG) + ' RPG: ' + str(self.RPG) + ' APG: ' + str(self.APG)


class GameStats:
    def __init__(self, PTS=None, REB=None, AST=None, seasonID=None, playerID=None, gameID=None, date=None, matchup=None,
                 WL=None, MIN=None, FGM=None, FGA=None, FG_PCT=None, FG3M=None, FG3A=None, FG3_PCT=None, FTM=None,
                 FTA=None, FT_PCT=None, OREB=None, DREB=None, STL=None, BLK=None, TO=None, PF=None, PLUS_MINUS=None):
        # checking against none to cast in proper format
        if PTS is not None:
            self.PTS = PTS
        else:
            self.PTS = None
        if REB is not None:
            self.REB = REB
        else:
            self.REB = None
        if AST is not None:
            self.AST = AST
        else:
            self.AST = None
        if seasonID is not None:
            self.seasonID = str(seasonID)
        else:
            self.seasonID = None
        if playerID is not None:
            self.playerID = str(playerID)
        else:
            self.playerID = None
        if gameID is not None:
            self.gameID = str(gameID)
        else:
            self.playerID = None
        if date is not None:
            self.date = str(date)
        else:
            self.playerID = None
        if matchup is not None:
            self.matchup = str(matchup)
        else:
            self.matchup = None
        if WL is not None:
            if str(WL) == 'W':
                self.result = 'Win'
            else:
                self.result = 'Loss'
        else:
            self.result = None
        if MIN is not None:
            self.minutes = int(MIN)
        else:
            self.minutes = None
        if FGM is not None:
            self.FGM = int(FGM)
        else:
            self.FMG = None
        if FGA is not None:
            self.FGA = int(FGA)
        else:
            self.FGA = None
        if FG_PCT is not None:
            self.FG_PCT = float(FG_PCT)
        else:  # calculate FG% if not input
            if self.FGM is not None and self.FGA is not None:
                try:
                    self.FG_PCT = self.FGM / self.FGA
                except ZeroDivisionError:
                    self.FG_PCT = 0
            else:
                self.FG_PCT = None
        if FG3M is not None:
            self.FG3M = int(FG3M)
        else:
            self.FG3M = None
        if FG3A is not None:
            self.FG3A = int(FG3A)
        else:
            self.FG3A = None
        if FG3_PCT is not None:
            self.FG3_PCT = float(FG3_PCT)
        else:  # calculate FG3% if not input
            if self.FG3M is not None and self.FG3A is not None:
                try:
                    self.FG3_PCT = self.FG3M / self.FG3A
                except ZeroDivisionError:
                    self.FG3_PCT = 0
            else:
                self.FG3_PCT = None
        if FTM is not None:
            self.FTM = int(FTM)
        else:
            self.FTM = None
        if FTA is not None:
            self.FTA = int(FTA)
        else:
            self.FTA = None
        if FT_PCT is not None:
            self.FT_PCT = float(FT_PCT)
        else:  # calculate FT% if not input
            if self.FTM is not None and self.FTA is not None:
                try:
                    self.FT_PCT = self.FTM / self.FTA
                except ZeroDivisionError:
                    self.FT_PCT = 0
            else:
                self.FT_PCT = None
        if OREB is not None:
            self.OREB = int(OREB)
        else:
            self.OREB = None
        if DREB is not None:
            self.DREB = int(DREB)
        else:
            self.DREB = None
        if self.OREB is None:
            if self.REB is not None and self.DREB is not None:
                self.OREB = self.REB - self.DREB
        if self.DREB is None:
            if self.REB is not None and self.OREB is not None:
                self.DREB = self.REB - self.OREB
        if self.REB is None:
            if self.OREB is not None and self.DREB is not None:
                self.REB = self.DREB + self.OREB
        if STL is not None:
            self.STL = int(STL)
        else:
            self.STL = None
        if BLK is not None:
            self.BLK = int(BLK)
        else:
            self.BLK = None
        if TO is not None:
            self.TO = int(TO)
        else:
            self.TO = None
        if PF is not None:
            self.PF = int(PF)
        else:
            self.PF = None
        if PLUS_MINUS is not None:
            self.PLUS_MINUS = int(PLUS_MINUS)

    def __str__(self):
        return str(self.matchup) + ', ' + self.date + ' - ' + str(self.PTS) + '/' + str(self.REB) + '/' + str(self.AST)


class Season:
    def __init__(self, season, games=None):
        self.season = str(season)
        self.games = []

        if games is not None:
            self.addGame(games)

        # update all stats
        if len(self.games) > 0:
            self.PTS = self.getSeasonPTS()
            self.PPG = self.getPPG()
            self.REB = self.getSeasonREB()
            self.RPG = self.getRPG()
            self.AST = self.getSeasonAST()
            self.APG = self.getAPG()

    def addGame(self, game):
        if type(game) is GameStats:
            self.games.append(game)
            return
        elif type(game) is list:
            for x in game:
                if type(x) is GameStats:
                    self.games.append(x)
                else:
                    raise TypeError('Games must be type GameStats')

    def getNumberOfGames(self, team):
        team = getTeamAbbreviation(team)
        number = 0
        for game in self.games:
            if team in game.matchup:
                number += 1
        return number

    def getSeasonPTS(self, team=None):
        totalPoints = 0
        if team is None:
            for game in self.games:
                totalPoints += game.PTS
        else:
            team = getTeamAbbreviation(team)
            for game in self.games:
                if team in game.matchup:
                    totalPoints += game.PTS

        return totalPoints

    def getPPG(self, team=None):
        if team is None:
            return self.PTS / len(self.games)
        else:
            team = getTeamAbbreviation(team)
            return round(self.getSeasonPTS(team) / self.getNumberOfGames(team), 2)

    def getSeasonREB(self, team=None):
        totalREB = 0
        if team is None:
            for game in self.games:
                totalREB += game.REB
        else:
            team = getTeamAbbreviation(team)
            for game in self.games:
                if team in game.matchup:
                    totalREB += game.REB

        return totalREB

    def getRPG(self, team=None):
        if team is None:
            return self.REB/len(self.games)
        else:
            team = getTeamAbbreviation(team)
            return round(self.getSeasonREB(team) / self.getNumberOfGames(team), 2)

    def getSeasonAST(self, team=None):
        totalAST = 0
        if team is None:
            for game in self.games:
                totalAST += game.AST
        else:
            team = getTeamAbbreviation(team)
            for game in self.games:
                if team in game.matchup:
                    totalAST += game.AST
        return totalAST

    def getAPG(self, team=None):
        if team is None:
            return self.AST/len(self.games)
        else:
            team = getTeamAbbreviation(team)
            return round(self.getSeasonAST(team) / self.getNumberOfGames(team), 2)

    def updateStats(self):
        self.PTS = self.getSeasonPTS()
        self.PPG = self.getPPG()
        self.REB = self.getSeasonREB()
        self.RPG = self.getRPG()
        self.AST = self.getSeasonAST()
        self.APG = self.getAPG()

    def __str__(self):
        return self.season + ': ' + str(self.PPG) + '/' + str(self.RPG) + '/' + str(self.APG) + ', ' + str(len(self.games)) + ' games played'


def getTeamAbbreviation(teamName):
    teamName = str(teamName).lower()
    for team in team_dict:
        if teamName in list(team['full_name'].lower().split(' ')) or team['abbreviation'].lower() == teamName:
            return team['abbreviation'].upper()
    raise Exception('Team not found')


def getTime(name):
    start = time.time()
    player = Player(name)
    print(time.time() - start)
    return player


dame = getTime('Damian Lillard')
#  jordan = Player('Michael Jordan')