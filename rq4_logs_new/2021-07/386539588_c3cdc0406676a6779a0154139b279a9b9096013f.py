from pandas import read_excel
from datetime import datetime
import numpy as np
import config
import MySQLdb

MAX_FLASH = 100


def get_database_connection():
    """connects to the MySQL database and returns the connection"""
    return MySQLdb.connect(host=config.MYSQL_HOST,
                           user=config.MYSQL_USERNAME,
                           passwd=config.MYSQL_PASSWORD,
                           db=config.MYSQL_DB_NAME,
                           charset='utf8')


def import_to_database_from_excel(filepath):
    '''Get a excel file name and imports data from it.'''

    db = get_database_connection()
    cur = db.cursor()

    total_flashes = 0
    output = []

    try:
        cur.execute('DROP TABLE IF EXISTS logs;')
        cur.execute("""CREATE TABLE logs (            
            log_name CHAR(200),
            log_value MEDIUMTEXT);
            """)
        db.commit()
    except Exception as e:
        print("dropping logs")
        output.append(
            f'problem dropping and creating new table for logs in database; {e}')

    # remove the E_SCORE table if exists, then create the new one
    try:
        cur.execute('DROP TABLE IF EXISTS E_SCORE;')
        cur.execute("""CREATE TABLE E_SCORE (
            id INTEGER PRIMARY KEY,
            d_date VARCHAR(200),
            USD FLOAT,EUR FLOAT,GBP FLOAT,JPY FLOAT,NZD FLOAT,AUD FLOAT,
            CHF FLOAT,CAD FLOAT,TRY FLOAT,MXN FLOAT,ZAR FLOAT,SEK FLOAT,
            DKK FLOAT,PLN FLOAT,SGD FLOAT,CZK FLOAT,HKD FLOAT,HUF FLOAT,
            NOK FLOAT,RUB FLOAT,THB FLOAT,CHN FLOAT);""")
        db.commit()
    except Exception as e:
        print("problem dropping E_SCORE")
        output.append(
            f'problem dropping and creating new table E_SCORE in database; {e}')
    
    cur.execute("INSERT INTO logs VALUES ('db_filename', 'E_SCORE CREATED')")
    db.commit()

    try:
        cur.execute('DROP TABLE IF EXISTS IR;')
        cur.execute("""CREATE TABLE IR (
            id INTEGER PRIMARY KEY,
            d_date VARCHAR(200),
            USD FLOAT,EUR FLOAT,GBP FLOAT,JPY FLOAT,NZD FLOAT,AUD FLOAT,
            CHF FLOAT,CAD FLOAT,TRY FLOAT,MXN FLOAT,ZAR FLOAT,SEK FLOAT,
            DKK FLOAT,PLN FLOAT,SGD FLOAT,CZK FLOAT,HKD FLOAT,HUF FLOAT,
            NOK FLOAT,RUB FLOAT,THB FLOAT,CHN FLOAT,Brazil FLOAT);""")
        db.commit()
    except Exception as e:
        print("problem dropping IR")
        output.append(
            f'problem dropping and creating new table IR in database; {e}')
    
    cur.execute("INSERT INTO logs VALUES ('db_filename', 'IR CREATED')")
    db.commit()

    try:
        cur.execute('DROP TABLE IF EXISTS GDP;')
        cur.execute("""CREATE TABLE GDP (
            id INTEGER PRIMARY KEY,
            d_date VARCHAR(200),
            USD FLOAT,EUR FLOAT,GBP FLOAT,JPY FLOAT,NZD FLOAT,AUD FLOAT,
            CHF FLOAT,CAD FLOAT,TRY FLOAT,MXN FLOAT,ZAR FLOAT,SEK FLOAT,
            DKK FLOAT,PLN FLOAT,SGD FLOAT,CZK FLOAT,HKD FLOAT,HUF FLOAT,
            NOK FLOAT,RUB FLOAT,THB FLOAT,CHN FLOAT);""")
        db.commit()
    except Exception as e:
        print("problem dropping excel")
        output.append(
            f'problem dropping and creating new table excel in database; {e}')
    
    cur.execute("INSERT INTO logs VALUES ('db_filename', 'GDP CREATED')")
    db.commit()

    try:
        cur.execute('DROP TABLE IF EXISTS COT;')
        cur.execute("""CREATE TABLE COT (
            id INTEGER PRIMARY KEY,
            d_date VARCHAR(200),
            L_USD FLOAT,S_USD FLOAT,A_USD FLOAT,L_EUR FLOAT,S_EUR FLOAT,A_EUR FLOAT,
            L_GBP FLOAT,S_GBP FLOAT,A_GBP FLOAT,L_JPY FLOAT,S_JPY FLOAT,A_JPY FLOAT,
            L_NZD FLOAT,S_NZD FLOAT,A_NZD FLOAT,L_AUD FLOAT,S_AUD FLOAT,A_AUD FLOAT,
            L_CHF FLOAT,S_CHF FLOAT,A_CHF FLOAT,L_CAD FLOAT,S_CAD FLOAT,A_CAD FLOAT,
            L_MXN FLOAT,S_MXN FLOAT,A_MXN FLOAT,L_ZAR FLOAT,S_ZAR FLOAT,A_ZAR FLOAT,
            L_RUB FLOAT,S_RUB FLOAT,A_RUB FLOAT,L_REAL FLOAT,S_REAL FLOAT,A_REAL FLOAT
            );""")
        db.commit()
    except Exception as e:
        print("problem dropping COT")
        output.append(
            f'problem dropping and creating new table COT in database; {e}')
    
    cur.execute("INSERT INTO logs VALUES ('db_filename', 'COT CREATED')")
    db.commit()

    df = read_excel(filepath, 0)
    df1 = df.replace(np.nan, 'NULL', regex=True)
    score_counter = 1
    line_number = 1
    for _, (d_date, USD, EUR, GBP, JPY, NZD, AUD,
            CHF, CAD, TRY, MXN, ZAR, SEK, DKK, PLN, SGD,
            CZK, HKD, HUF, NOK, RUB, THB, CHN) in df1.iterrows():
        line_number += 1
        try:
            cur.execute(f'''INSERT INTO E_SCORE VALUES ({line_number}, '{d_date}',
                {USD},{EUR}, {GBP}, {JPY}, {NZD}, {AUD}, {CHF}, {CAD}, {TRY},
                {MXN}, {ZAR}, {SEK}, {DKK}, {PLN}, {SGD}, {CZK}, {HKD},
                {HUF}, {NOK}, {RUB}, {THB}, {CHN});''')
            score_counter += 1
        except Exception as e:
            total_flashes += 1
            if total_flashes < MAX_FLASH:                
                output.append(
                    f'Error inserting line {line_number} from E_SCORE sheet , {e}')
            elif total_flashes == MAX_FLASH:
                output.append(f'Too many errors!')
        if line_number % 1000 == 0:
            try:
                db.commit()
            except Exception as e:
                output.append(
                    f'Problem commiting data into db at around record {line_number} (or previous 1000 ones); {e}')
    db.commit()
    df = read_excel(filepath, 1)
    df1 = df.replace(np.nan, 'NULL', regex=True)
    gdp_counter = 1
    line_number = 1
    for _, (d_date, USD, EUR, GBP, JPY, NZD, AUD,
            CHF, CAD, TRY, MXN, ZAR, SEK, DKK, PLN, SGD,
            CZK, HKD, HUF, NOK, RUB, THB, CHN) in df1.iterrows():
        line_number += 1
        try:
            cur.execute(f'''INSERT INTO GDP VALUES ({line_number}, '{d_date}',
                {USD},{EUR}, {GBP}, {JPY}, {NZD}, {AUD}, {CHF}, {CAD}, {TRY},
                {MXN}, {ZAR}, {SEK}, {DKK}, {PLN}, {SGD}, {CZK}, {HKD},
                {HUF}, {NOK}, {RUB}, {THB}, {CHN});''')
            gdp_counter += 1
        except Exception as e:
            total_flashes += 1
            if total_flashes < MAX_FLASH:                
                output.append(
                    f'Error inserting line {line_number} from GDP sheet , {e}')
            elif total_flashes == MAX_FLASH:
                output.append(f'Too many errors!')
        if line_number % 1000 == 0:
            try:
                db.commit()
            except Exception as e:
                output.append(
                    f'Problem commiting data into db at around record {line_number} (or previous 1000 ones); {e}')
    db.commit()
    df = read_excel(filepath, 2, 2)
    df1 = df.replace(np.nan, 'NULL', regex=True)
    cot_counter = 1
    line_number = 1
    for _, (t_date, usdl, usds, usda, eurl, eurs, eura,
            gbpl, gbps, gbpa, jpyl, jpys, jpya, nzdl, nzds, nzda,
            audl, auds, auda, chfl, chfs, chfa, cadl, cads, cada,
            mxnl, mxns, mxna, zarl, zars, zara, rubl, rubs, ruba,
            reall, reals, reala) in df1.iterrows():
        line_number += 1
        try:
            cur.execute(f'''INSERT INTO COT VALUES ({line_number}, '{t_date}',
                {usdl}, {usds}, {usda}, {eurl}, {eurs}, {eura}, {gbpl}, {gbps},
                {gbpa}, {jpyl}, {jpys}, {jpya}, {nzdl}, {nzds}, {nzda}, {audl},
                {auds}, {auda}, {chfl}, {chfs}, {chfa}, {cadl}, {cads}, {cada},
                {mxnl}, {mxns}, {mxna}, {zarl}, {zars}, {zara}, {rubl},
                {rubs}, {ruba}, {reall}, {reals}, {reala});''')
            cot_counter += 1
        except Exception as e:
            total_flashes += 1
            if total_flashes < MAX_FLASH:                
                output.append(
                    f'Error inserting line {line_number} from COT sheet , {e}')
            elif total_flashes == MAX_FLASH:
                output.append(f'Too many errors!')
        if line_number % 1000 == 0:
            try:
                db.commit()
            except Exception as e:
                output.append(
                    f'Problem commiting data into db at around record {line_number} (or previous 1000 ones); {e}')
    db.commit()
    df = read_excel(filepath, 3)
    df1 = df.replace(np.nan, 'NULL', regex=True)
    ir_counter = 1
    line_number = 1
    for _, (d_date, USD, EUR, GBP, JPY, NZD, AUD,
            CHF, CAD, TRY, MXN, ZAR, SEK, DKK, PLN, SGD,
            CZK, HKD, HUF, NOK, RUB, THB, CHN, BRZ) in df1.iterrows():
        line_number += 1
        try:
            cur.execute(f'''INSERT INTO IR VALUES ({line_number}, '{d_date}',
                {USD},{EUR}, {GBP}, {JPY}, {NZD}, {AUD}, {CHF}, {CAD}, {TRY},
                {MXN}, {ZAR}, {SEK}, {DKK}, {PLN}, {SGD}, {CZK}, {HKD},
                {HUF}, {NOK}, {RUB}, {THB}, {CHN}, {BRZ});''')
            ir_counter += 1
        except Exception as e:
            total_flashes += 1
            if total_flashes < MAX_FLASH:                
                output.append(
                    f'Error inserting line {line_number} from IR sheet , {e}')
            elif total_flashes == MAX_FLASH:
                output.append(f'Too many errors!')
        if line_number % 1000 == 0:
            try:
                db.commit()
            except Exception as e:
                output.append(
                    f'Problem commiting data into db at around record {line_number} (or previous 1000 ones); {e}')
    db.commit()
     # save the logs
    output.append(f'Inserted {score_counter} values to score')
    output.append(f'Inserted {gdp_counter} values to GDP')
    output.append(f'Inserted {cot_counter} values to COT')
    output.append(f'Inserted {ir_counter} values to IR')
    output.reverse()
    cur.execute("INSERT INTO logs VALUES ('import', %s)", ('\n'.join(output), ))
    db.commit()

    db.close()

    return


import_to_database_from_excel('test.xls')