#!/usr/bin/env python3
# coding: utf-8

import csv
import ftplib
from datetime import datetime
import requests
from apscheduler.schedulers.blocking import BlockingScheduler

FTP_HOST = 'IP'
FTP_PORT = 8821
FTP_LOGIN = 'name'
FTP_PASS = 'password'
FTP_DIR_PATH = '/IP/BEC/Config'
FTP_FILE_NAME = 'WhiteList.txt'

LOCAL_WHITELIST_FILE_NAME = 'WhiteList.txt'
CSV_URL = ''

TRIES = 5  # Maximum tries.
SCORE = '21 / 21'  # String eval to avoid any string conversion. Format:  'x / x'
BASESTRING = ''  # Hardcoded values for output. For lazy admins/backup.


class PlayerRecord:
    """DATA class."""

    def __init__(self, lastdate, date, name, score, guid, tries):
        self.lastdate = lastdate
        self.date = date
        self.name = name
        self.score = score
        self.guid = guid
        self.tries = int(tries)


def upload_whitelist():
    print('FTP connecting...')
    ftp = ftplib.FTP()

    ftp.connect(FTP_HOST, FTP_PORT)
    print(ftp.getwelcome())

    try:
        ftp.login(FTP_LOGIN, FTP_PASS)
        print("Logged in.")
    except:
        print("Failed to login.")

    ftp.cwd(FTP_DIR_PATH)

    with open(FTP_FILE_NAME, 'rb') as file:
        this = ftp.storbinary('STOR %s' % FTP_FILE_NAME, file)
        print(this)

    ftp.quit()
    print('FTP disconnected.')


def get_spreadsheet_data():
    # download CSV
    with requests.Session() as s:
        download = s.get(CSV_URL)
        decoded_content = download.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        values = list(cr)
        values = values[1:]  # We skip the first line containing row names/titles.

        if not values:
            print('No data found.')
        else:
            player_records = []
            player_record_id = {}
            valid_names = []
            for row in values:
                # Counts number of tries, remembers last score and date.
                if row[4] in player_record_id:
                    player_records[player_record_id[row[4]]].tries += 1
                    player_records[player_record_id[row[4]]].score = row[2]
                    player_records[player_record_id[row[4]]].lastdate = row[0]
                else:
                    # Creates a player record and a dictionary entry for reverse search.
                    if len(row[4]) == 32:
                        player_records.append(PlayerRecord(row[0], row[0], row[5], row[2], row[4], 1))
                        player_record_id[row[4]] = len(player_records) - 1

            # Evaluates unique member in the dictionary.
            for key in player_record_id.keys():
                #   print(key, ":", player_record_id[key])
                #   print(player_records[player_record_id[key]].tries)
                #   print(player_records[player_record_id[key]].score)

                if player_records[player_record_id[key]].tries < TRIES + 1:
                    if player_records[player_record_id[key]].score == SCORE:
                        valid_names.append(player_records[player_record_id[key]].guid +
                                           ' ' +
                                           player_records[player_record_id[key]].name +
                                           '\n')

            data = BASESTRING + '\n' + ''.join(valid_names)
            data = data.encode()
            print('Spreadsheet downloaded to memory successfully.')

            try:
                with open(LOCAL_WHITELIST_FILE_NAME, 'rb') as afile:
                    if afile.read() == data:
                        print('Data not changed, no FTP update necessary.')
                        print('Retrying in an hour. \n')
                        return False
                    else:
                        update_local(data)
                        return True
            except:
                error_logger('Local WhiteList File was not found. Error Logged.')
                update_local(data)
                return True


def update_local(data):
    with open(LOCAL_WHITELIST_FILE_NAME, 'wb') as file:
        file.write(data)
        print('Local update finished.')


def job():
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

    print('Getting WhiteList data from spreadsheet.', dt_string)
    if get_spreadsheet_data():
        upload_whitelist()


def error_logger(err_details: str):
    print(str)
    with open('error.log', 'a') as afile:
        now = datetime.now()
        err_string = now.strftime("%d/%m/%Y %H:%M:%S") + err_details
        afile.write(err_string)


def my_scheduler():
    job()
    scheduler = BlockingScheduler()
    scheduler.add_job(job, 'interval', hours=1)
    scheduler.start()


def main():
    my_scheduler()


if __name__ == '__main__':
    main()
