import csv
import os
import sys
from collections import deque


def save(filename: str, games: deque, mode: str):
    columns = (
        'Event',
        'ID',
        'White',
        'Black',
        'Result',
        'UTCDate',
        'UTCTime',
        'WhiteElo',
        'BlackElo',
        'WhiteRatingDiff',
        'BlackRatingDiff',
        'ECO',
        'Opening',
        'TimeControl',
        'Termination',
    )
    with open(f'{filename}.csv', mode) as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',')
        if mode == 'w':
            csv_writer.writerow(columns)
        for game in games:
            csv_writer.writerow(game)


def find_games(filename):
    half_mb = 500000
    with open(f'{filename}.pgn', 'r') as reader:
        lines = reader.readlines()
        games = deque()
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
                    print(f'({index:,} / {len(lines) - 1:,}) : {sys.getsizeof(games)}')
                    if sys.getsizeof(games) >= half_mb:
                        yield games
                        games.clear()
                    else:
                        game = []


def main():
    filename = 'chess/files/jan2013'
    try:
        os.remove(f'{filename}.csv')
    except FileNotFoundError:
        pass

    games = find_games(filename)
    for index, games_found in enumerate(games):
        if not index:
            save(filename, games_found, 'w')
        else:
            save(filename, games_found, 'a')


if __name__ == '__main__':
    main()
