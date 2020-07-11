#import fdb
import csv
import sys
import argparse

def get_con():
    con = fdb.connect(dsn='/home/willian/VINICIUS/gpimport/database.fdb', user='sysdba', password='masterkey')
    return con

def read_csv(file):
    with open(file, 'r') as f:
        reader = csv.DictReader(f)
        return [item for item in reader]

def get_sql_command(items, tablename):
    commands = []
    for item in items:

        columns = ','.join(item.keys())
        values = ",".join([f"'{i}'" for i in item.values()])

        sql = f"INSERT INTO {tablename} ({columns}) values({values});"
        commands.append(sql)
    return commands

def save_on_db(commands):
    con = get_con()
    cur = con.cursor()
    for sql in commands:
        cur.execute(sql)
    con.commit()

def select(tablename):
    cur =get_con().cursor()
    cur.execute(f'select * from {tablename};')
    return cur.fetchall()

def write_file(file_output, commands):
    with open(file_output, 'w') as f:
        for sql in commands:
            print(sql)
            f.write(f'{sql}\n')


def main():
    parser = argparse.ArgumentParser(description='Gerador de sql')
    parser.add_argument('--filein',  help='arquivo de entrada')
    parser.add_argument('--fileout', help='arquivo de saida')
    parser.add_argument('--tablename', help='nome da tabela')
    args = parser.parse_args()

    if args.filein is None or args.fileout is None or args.tablename is None:
        parser.print_help()
    else:
        items = read_csv(args.filein)
        commands = get_sql_command(items, args.tablename)
        write_file(args.fileout, commands)


if __name__ == '__main__':
    main()

