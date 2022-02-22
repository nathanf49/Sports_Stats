from urllib.request import urlopen

import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
import pandas as pd
import NFL_radarCharts as radar

### TODO error when trying to print a second radar chart whether using data = data[1:] or trying to use filterTeams filterTeams also makes the graph inaccurate, but just running the program to print works fine

def filterTeams(team, data):
    """Filters data frame to just include teams in input"""
    if type(team) is list:
        return data[data.Tm.isin(team)]  # only keeps players in list of strs of team

    elif type(team) is str:
        for player in range(len(data)):
            if data.iloc[player]['Tm'] == team:
                return data[data.Tm == team]  # only keeps players in str of team

    else:
        raise Exception('Please input the team as a str or a list of strs')


def getStats():
    statsURL = 'https://www.pro-football-reference.com/years/2021/passing.htm'
    html = urlopen(statsURL)
    statsPage = BeautifulSoup(html, features='html5lib')

    headers = statsPage.findAll('tr')[0]  # tr is table rows, th is table headers, td is columns
    headers = [i.getText() for i in headers.findAll('th')]  # gets header for each row
    headers = headers[1:]  # skip rank. not imported with data and list is already in order

    rows = statsPage.findAll('tr')[1:]  # finds all rows after headers, but does not take text out yet
    qbStats = []

    for i in range(len(rows)):  # go through each row and collect the text
        playerStats = [col.getText() for col in rows[i].findAll('td')]
        try:
            if playerStats[3] in ['QB', 'qb', 'Qb']:  # make sure player is a qb
                qbStats.append(playerStats)
            # if playerStats[0][-1] == '*' or playerStats[0][-1] == '+':
            #    playerStats[0] = playerStats[0][0:-1]

        except IndexError:  # line is not for a player
            pass

    data = pd.DataFrame(qbStats, columns=headers)  # create dataframe for data
    newColumns = data.columns.values  # Yds appears for throwing yards and sack yards. Change sack yards
    newColumns[-6] = 'SackYardage'
    data.columns = newColumns
    data['Player'] = data['Player'].str.replace('*', '')  # get rid of * and +
    #  data = data['Player'].str.repeat("+", '')

    return data
    # filterData(data)


if __name__ == '__main__':
    QBs = getStats()
    teams = ['NWE', 'TAM']
    # QBs = filterTeams(teams, QBs)  #TODO makes graph inaccurate. Too square
    pltData = radar.filterData(QBs)

    fig = plt.figure(figsize=(8, 8), facecolor='white')  # create figure with white background
    ax1 = fig.add_subplot(221, projection='polar', facecolor='#ededed')
    ax2 = fig.add_subplot(222, projection='polar', facecolor='#ededed')
    # ax3 = fig.add_subplot(223, projection='polar', facecolor='#ededed')
    # ax4 = fig.add_subplot(224, projection='polar', facecolor='#ededed')

    plt.subplots_adjust(hspace=0.8, wspace=0.5)  # adjusts space between subplots

    # plot data
    ax1 = radar.create_radar_chart(ax1, pltData[0], pltData[1], pltData[2])
    pltData[1] = pltData[1].iloc[1:]  ### TODO automatically prints top player, need to move to second player
    # ax2 = radar.create_radar_chart(ax2, pltData[0], pltData[1], pltData[2])
    plt.show()
