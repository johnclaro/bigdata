import csv
import os
import sys
from collections import deque

from tqdm import tqdm


def save(filename: str, games: deque):
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
    with open(f'{filename}.csv', 'w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',')
        csv_writer.writerow(columns)
        for game in games:
            csv_writer.writerow(game)


def parse(filename):
    with open(f'{filename}.pgn', 'r') as reader:
        lines = reader.readlines()
        games = deque()
        game = []
        for line in tqdm(lines):
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

    save(filename, games)


def main():
    filename = 'chess/datasets/jan2013'
    one_gb = 1073741824
    size = os.path.getsize(f'{filename}.pgn')
    if size >= one_gb:
        sys.exit('File exceeds 1GB, closing...')
    parse(filename)


if __name__ == '__main__':
    main()
