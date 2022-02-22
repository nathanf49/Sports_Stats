import csv

### TODO - Find stats for each week for players
def percentage(players):
    """
    Calculates and prints the average win percentage and overall wins of each player, team, conference, and position
    """
    minToPrint = 10
    playerCount = {}
    teamCount = {}
    conferenceCount = {}
    positionCount = {}
    startingYear = 2022

    for p in players:
        if p['Player'] in playerCount:  # individual players
            playerCount[p['Player']] += 1
        else:
            playerCount[p['Player']] = 1

        if p['Team'] in teamCount:
            teamCount[p['Team']] += 1
        else:
            teamCount[p['Team']] = 1

        if p['Conference'] in conferenceCount:
            conferenceCount[p['Conference']] += 1
        else:
            conferenceCount[p['Conference']] = 1

        if p['Position'] in positionCount:
            positionCount[p['Position']] += 1
        else:
            positionCount[p['Position']] = 1

        if int(p['Date'].split(',')[-1]) < startingYear:
            startingYear = int(p['Date'].split(',')[-1])

    print('Starting in: ' + str(startingYear))
    print('\n')

    print('Players:')
    for player in playerCount:
        if playerCount[player] > minToPrint:
            percentage = "{:.2%}".format((playerCount[player] / sum(playerCount.values())))
            print(str(player) + ': ' + str(playerCount[player]) + ' Awards, ' + str(percentage))

    print('\n')
    print('Teams:')
    for team in teamCount:
        if teamCount[team] != 0:
            percentage = "{:.2%}".format(teamCount[team] / sum(teamCount.values()))
            print(str(team) + ': ' + str(teamCount[team]) + ' Awards, ' + str(percentage))

    print('\n')
    print('Conferences:')
    for conference in conferenceCount:
        percentage = "{:.2%}".format(conferenceCount[conference] / sum(conferenceCount.values()))
        print(str(conference) + ': ' + str(conferenceCount[conference]) + ' Awards, ' + str(percentage))

    print('\n')
    print('Positions')
    for position in positionCount:
        percentage = "{:.2%}".format(positionCount[position] / sum(positionCount.values()))
        print(str(position) + ': ' + str(positionCount[position]) + ' Awards, ' + str(percentage))



if __name__ == '__main__':
    file = 'NBA_player_of_the_week.csv'
    with open(file) as f:
        data = csv.DictReader(f)
        percentage(data)

