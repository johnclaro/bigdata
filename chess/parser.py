import csv
from collections import deque


def main():
    columns = (
        'Event', 'ID', 'White', 'Black', 'Result', 'UTCDate', 'UTCTime',
        'WhiteElo', 'BlackElo', 'WhiteRatingDiff', 'BlackRatingDiff', 'ECO',
        'Opening', 'TimeControl', 'Termination'
    )
    games = deque()
    filename = 'chess/files/july2016'
    with open(f'{filename}.pgn', 'r') as reader:
        lines = reader.readlines()
        game = []
        for index, line in enumerate(lines):
            key = line.split('"')[0]
            if '[' in key and '1. ' not in key[:3]:
                key = key.replace('[', '').replace(' ', '')
                value = line.split('"')[1]
                if key == 'Site':
                    value = value.replace('https://lichess.org/', '')
                elif key == 'Event':
                    value = value.replace('Rated ', '').replace(' game', '')
                game.append(value)
                if key == 'Termination':
                    games.append(game)
                    game = []

    with open(f'{filename}.csv', 'w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',')
        csv_writer.writerow(columns)
        for game in games:
            csv_writer.writerow(game)


if __name__ == '__main__':
    main()
