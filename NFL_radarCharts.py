import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt


def filterData(data):
    """ Filter data for radar graph"""
    categories = ['Cmp%', 'Yds', 'TD', 'Int', 'Y/A', 'Rate']
    radarData = data[['Player', 'Tm'] + categories]  # filter data

    for i in categories:  # converts pandas objects to numbers
        radarData[i] = pd.to_numeric(data[i])
    filterRadarData = radarData[radarData['Yds'] > 1500]  # get rid of any qb with less than 1500 yards

    for i in categories:  # creates percentile ranks for each column
        filterRadarData[i + '_Rank'] = filterRadarData[i].rank(pct=True)
    filterRadarData['Int_Rank'] = 1 - filterRadarData['Int_Rank']  # ints are bad, so they should be flipped

    # matplotlib.rcParams['font.family'] = 'Avenir'
    matplotlib.rcParams['font.size'] = 16
    matplotlib.rcParams['axes.linewidth'] = 0  # x ticks correspond to the angle around the circle
    matplotlib.rcParams['xtick.major.pad'] = 15  # increase padding between axis and tick labels

    offset = np.pi/6  # Calculate angles for radar chart
    angles = np.linspace(0, 2 * np.pi, len(categories) + 1) + offset
    # instead of the first label at 0 radians, it now appears at π/6
    return [angles, filterRadarData, categories]


def create_radar_chart(ax, angles, player_data, categories):
    team_colors = {'ARI': '#97233f', 'ATL': '#a71930', 'BAL': '#241773', 'BUF': '#00338d', 'CAR': '#0085ca',
                   'CHI': '#0b162a', 'CIN': '#fb4f14', 'CLE': '#311d00', 'DAL': '#041e42', 'DEN': '#002244',
                   'DET': '#0076b6', 'GNB': '#203731', 'HOU': '#03202f', 'IND': '#002c5f', 'JAX': '#006778',
                   'KAN': '#e31837', 'LAC': '#002a5e', 'LAR': '#003594', 'MIA': '#008e97', 'MIN': '#4f2683',
                   'NWE': '#002244', 'NOR': '#d3bc8d', 'NYG': '#0b2265', 'NYJ': '#125740', 'OAK': '#000000',
                   'PHI': '#004c54', 'PIT': '#ffb612', 'SFO': '#aa0000', 'SEA': '#002244', 'TAM': '#d50a0a',
                   'TEN': '#0c2340', 'WAS': '#773141'}

    color = team_colors[player_data['Tm'][0]]
    player_data = np.asarray(player_data)[0]  # automatically plots top player
    # Plot data and fill with team color
    ax.plot(angles, np.append(player_data[-(len(angles) - 1):], player_data[-len(angles) - 1]), color=color, linewidth=2)
    ax.fill(angles, np.append(player_data[-(len(angles) - 1):], player_data[-len(angles) - 1]), color=color, alpha=0.2)

    # Set category labels
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories)

    # Remove radial labels
    ax.set_yticklabels([])

    # Add player name
    ax.text(np.pi / 2, 1.7, player_data[0], ha='center', va='center',size=18, color=color)

    # Use white grid
    ax.grid(color='white', linewidth=1.5)

    # Set axis limits
    ax.set(xlim=(0, 2 * np.pi), ylim=(0, 1))

    return ax