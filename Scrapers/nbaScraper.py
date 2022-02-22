# scrapes data from nba stats website. Might want to try basketball reference as well
import requests
import pandas as pd


#  TODO test other parameters in url

#  keep these headers, they're needed for access  -(video from 3/21) https://www.youtube.com/watch?v=IELK56jIsEo
headers = {
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'x-nba-stats-token': 'true',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
    'x-nba-stats-origin': 'stats',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Referer': 'https://stats.nba.com/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
}


def column_filter(data, cols_wanted):
    """ Allows user to specify columns wanted instead of returning the entire data frame """
    returnData = pd.DataFrame()  # make a data frame to store cols wanted in
    if type(cols_wanted) is list:
        for col in cols_wanted:  # if cols wanted is a list, get all cols_wanted and replace payerInfo with them
            if col in data:
                returnData[col] = data[col]

    else:
        returnData[cols_wanted] = data[cols_wanted]  # if only one column is specified, return it

    return returnData  # returns new data frame with only specified columns


def getStats(season='2021-22', stat_type=1, per_mode='Totals', cols_wanted='All'):
    """ Gets totals of standard and/or advanced stats from current season by default.
        Can also get Per_Game, Per100Posessions, Per36
        and other seasons. Can probably insert other parameters in the url, but that still has to be tested """
    global headers
    if stat_type == 'Traditional':
        statsURL = 'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=' + per_mode + '&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=' + season + '&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision=&Weight='
        response = requests.get(url=statsURL, headers=headers).json()
        playerInfo = pd.DataFrame(response['resultSets'][0]['rowSet'], columns=response['resultSets'][0]['headers'])

    elif stat_type == 'Advanced':
        advancedStatsURL = 'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=' + per_mode + '&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=' + season + '&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision=&Weight='
        response_advanced = requests.get(url=advancedStatsURL, headers=headers).json()
        playerInfo = pd.DataFrame(response_advanced['resultSets'][0]['rowSet'], columns=response_advanced['resultSets'][0]['headers'])

    else:
        statsURL = 'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=' + per_mode + '&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=' + season + '&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision=&Weight='
        response = requests.get(url=statsURL, headers=headers).json()
        playerInfo = pd.DataFrame(response['resultSets'][0]['rowSet'], columns=response['resultSets'][0]['headers'])

        advancedStatsURL = 'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=' + per_mode + '&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=' + season + '&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision=&Weight='
        response_advanced = requests.get(url=advancedStatsURL, headers=headers).json()
        advancedStats = pd.DataFrame(response_advanced['resultSets'][0]['rowSet'], columns=response_advanced['resultSets'][0]['headers'])

        # combine data frames into 1
        for col in response_advanced['resultSets'][0]['headers']:
            if col not in response['resultSets'][0]['headers']:
                playerInfo[col] = advancedStats[col]

    if cols_wanted != 'All':
        playerInfo = column_filter(playerInfo, cols_wanted)

    return playerInfo.drop('NICKNAME', axis=1)  # return dataframe without nickname column (first name)


createTableQuery = """CREATE TABLE Combined_Stats2 (
                   PLAYER_ID int(15) NOT NULL, 
                   PLAYER_NAME varchar(50) NOT NULL, 
                   TEAM_ID int(15), 
                   TEAM_ABBREVIATION varchar(5) NOT NULL,
                   AGE int(3) NOT NULL,
                   GP int(5) NOT NULL,
                   W int(5) NOT NULL,
                   L int(5) NOT NULL,
                   W_PCT float(5) NOT NULL,
                   MIN float(7) NOT NULL,
                   FGM int(7) NOT NULL,
                   FGA int(7) NOT NULL,
                   FG_PCT float(5) NOT NULL,
                   FG3M int(7) NOT NULL,
                   FG3A int(7) NOT NULL,
                   FG3_PCT float(5) NOT NULL,
                   FTM int(7) NOT NULL,
                   FTA int(7) NOT NULL,
                   FT_PCT float(5) NOT NULL,
                   OREB int(7) NOT NULL,
                   DREB int(7) NOT NULL,
                   REB int(7) NOT NULL,
                   AST int(7) NOT NULL,
                   TOV int(7) NOT NULL,
                   STL int(7) NOT NULL,
                   BLK int(7) NOT NULL,
                   BLKA int(7) NOT NULL,
                   PF int(7) NOT NULL,
                   PFD int(7) NOT NULL,
                   PTS int(7) NOT NULL,
                   PLUS_MINUS int(7) NOT NULL,
                   NBA_FANTASY_PTS float(10) NOT NULL,
                   DD2 int(5) NOT NULL,
                   TD3 int(3) NOT NULL,
                   GP_RANK int(7) NOT NULL,
                   W_RANK int(7) NOT NULL,
                   L_RANK int(7) NOT NULL,
                   W_PCT_RANK int(7) NOT NULL,
                   MIN_RANK int(7) NOT NULL,
                   FGM_RANK int(7) NOT NULL,
                   FGA_RANK int(7) NOT NULL,
                   FG_PCT_RANK int(7) NOT NULL,
                   FG3M_RANK int(7) NOT NULL,
                   FG3A_RANK int(7) NOT NULL,
                   FG3_PCT_RANK int(7) NOT NULL,
                   FTM_RANK int(7) NOT NULL,
                   FTA_RANK int(7) NOT NULL,
                   FT_PCT_RANK int(7) NOT NULL,
                   OREB_RANK int(7) NOT NULL,
                   DREB_RANK int(7) NOT NULL,
                   REB_RANK int(5) NOT NULL,
                   AST_RANK int(5) NOT NULL,
                   TOV_RANK int(5) NOT NULL,
                   STL_RANK int(5) NOT NULL,
                   BLK_RANK int(5) NOT NULL,
                   BLKA_RANK int(5) NOT NULL,
                   PF_RANK int(5) NOT NULL,
                   PFD_RANK int(5) NOT NULL,
                   PTS_RANK iNT(5) NOT NULL,
                   PLUS_MINUS_RANK int(5) NOT NULL,
                   NBA_FANTASY_PTS_RANK int(5) NOT NULL,
                   DD2_RANK int(5) NOT NULL,
                   TD3_RANK int(5) NOT NULL,
                   CFID int(5) NOT NULL,
                   CFPARAMS varchar(100) NOT NULL,
                   E_OFF_RATING float(10) NOT NULL,
                   OFF_RATING float(10) NOT NULL,
                   sp_work_OFF_RATING float(10) NOT NULL,
                   E_DEF_RATING float(10) NOT NULL,
                   DEF_RATING float(10) NOT NULL,
                   sp_work_DEF_RATING float(10) NOT NULL,
                   E_NET_RATING float(10) NOT NULL,
                   NET_RATING float(10) NOT NULL,
                   sp_work_NET_RATING float(10) NOT NULL,
                   AST_PCT float(10) NOT NULL,
                   AST_TO float(10) NOT NULL,
                   AST_RATIO float(10) NOT NULL,
                   OREB_PCT float(10) NOT NULL,
                   DREB_PCT float(10) NOT NULL,
                   TM_TO_PCT float(10) NOT NULL,
                   E_TOV_PCT float(10) NOT NULL,
                   EFG_PCT float(10) NOT NULL,
                   TS_PCT float(10) NOT NULL,
                   USG_PCT float(10) NOT NULL,
                   E_USG_PCT float(10) NOT NULL,
                   E_PACE float(10) NOT NULL,
                   PACE float(10) NOT NULL,
                   PACE_PER40 float(10) NOT NULL,
                   sp_work_PACE float(10) NOT NULL, 
                   PIE float(10) NOT NULL, 
                   POSS int(10) NOT NULL, 
                   FGM_PG float(10) NOT NULL,
                   FGA_PG float(10) NOT NULL, 
                   E_OFF_RATING_RANK int(5) NOT NULL,
                   OFF_RATING_RANK int(5) NOT NULL, 
                   sp_work_OFF_RATING_RANK int(5) NOT NULL,
                   E_DEF_RATING_RANK int(5) NOT NULL, 
                   DEF_RATING_RANK int(5) NOT NULL, 
                   sp_work_DEF_RATING_RANK int(5) NOT NULL, 
                   E_NET_RATING_RANK int(5) NOT NULL,
                   NET_RATING_RANK int(5) NOT NULL, 
                   sp_work_NET_RATING_RANK int(5),
                   AST_PCT_RANK int(5) NOT NULL,
                   AST_TO_RANK int(5) NOT NULL,
                   AST_RATIO_RANK int(5) NOT NULL,
                   OREB_PCT_RANK int(5) NOT NULL,
                   DREB_PCT_RANK int(5) NOT NULL,
                   REB_PCT_RANK int(5) NOT NULL,
                   TM_TOV_PCT_RANK int(5) NOT NULL, 
                   E_TOV_PCT_RANK int(5) NOT NULL, 
                   EFG_PCT_RANK int(5) NOT NULL, 
                   TS_PCT_RANK int(5) NOT NULL, 
                   USG_PCT_RANK int(5) NOT NULL, 
                   E_USG_PCT_RANK int(5) NOT NULL, 
                   E_PACE_RANK int(5) NOT NULL, 
                   PACE_RANK int(5) NOT NULL, 
                   sp_work_PACE_RANK int(5) NOT NULL, 
                   PIE_RANK int(5) NOT NULL, 
                   FGM_PG_RANK int(5) NOT NULL, 
                   FGA_PG_RANK int(5) NOT NULL);"""
