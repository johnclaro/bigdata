import csv
import json


def main():
    ecos = {}
    with open('ecos/ecos.csv', mode='r') as ecos_csv:
        csv_reader = csv.DictReader(ecos_csv)
        for index, row in enumerate(csv_reader):
            if index:
                name = row['name']
                if name not in ecos.keys():
                    player = 'black' if len(row['moves']) % 2 else 'white'
                    ecos[name] = player

    with open('ecos/ecos.json', 'w') as ecos_json:
        json.dump(ecos, ecos_json, indent=4, sort_keys=True)


if __name__ == '__main__':
    main()
