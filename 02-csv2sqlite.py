#!/usr/bin/python

import csv
import sys
import datetime as dt
import sqlite3

db = sqlite3.connect('sqlite/export.sqlite3')

ph = lambda x,y: ' '.join([x,y])

def s2int(string):
    if string == None or string == '':
        return -1
    return int(string)

def s2real(string):
    if string == None or string == '':
        return -1
    return float(string)

def s2d(string):
    if string == None or string == '':
        return None
    return dt.datetime.strptime(string, "%m/%d/%y %H:%M:%S")

def d2s(date):
    return date.strftime("%Y-%m-%d_%H:%M:%S")

def csv_reader(f):
    return csv.DictReader(open(f, 'r', newline=''), delimiter=',', quotechar='"')

def db_create_table(table, fields):
    db.execute('''CREATE TABLE IF NOT EXISTS {table} ({fields})'''.format(table=table, fields=fields))

def db_insert(table, data):
    def gen_qm():
        return ','.join(("?")*len(data))
    db.execute("INSERT INTO {table} VALUES ({placeholders_len})".format(table=table,placeholders_len=gen_qm()), data)

def db_import(table, fields):
    reader = csv_reader(table + ".csv")

    def datatype(f):
        return f[0] + ' ' + f[1]

    db_create_table(table, ', '.join(map(datatype, fields)))

    for row in reader:
        data = []
        for field, dtype in fields:
            if dtype == 'TEXT':
                data.append(row[field])
            elif dtype == 'INT':
                data.append(s2int(row[field]))
            elif dtype == 'REAL':
                data.append(s2real(row[field]))
            elif dtype == 'DATETIME':
                data.append(s2d(row[field]))
            elif dtype == 'BOOLEAN':
                data.append(bool(row[field]))
            else:
                raise ValueError("No Format known for: " + dtype)

        db_insert(table,  data)

    for lines in db.execute("SELECT Count(*) FROM " + table):
        print("Table '{}' has {} lines.".format(table, lines))

    db.commit()

db_import("Adressen", [
            ('ID', 'INT'),
            ('PERSE', 'TEXT'),
            ('DERFASST', 'DATETIME'),
            ('ZERFASST', 'DATETIME'),
            ('PERSG', 'TEXT'),
            ('DGEAENDERT', 'DATETIME'),
            ('ZGEAENDERT', 'DATETIME'),
            ('GEHOERT', 'TEXT'),
            ('ISTPRIVAT', 'BOOLEAN'),
            ('HAUPT', 'TEXT'),
            ('VNUMMER', 'INT'),
            ('DUBIDENT', 'TEXT'),
            ('FIRMA1', 'TEXT'),
            ('ABTEILUNG', 'TEXT'),
            ('ANPERSON', 'TEXT'),
            ('NAME', 'TEXT'),
            ('POSITION', 'TEXT'),
            ('STRASSE', 'TEXT'),
            ('POSTFACH', 'TEXT'),
            ('LAND', 'TEXT'),
            ('PLZ', 'TEXT'),
            ('ORT', 'TEXT'),
            ('ANREDE', 'TEXT'),
            ('TELEFON', 'TEXT'),
            ('MOBIL', 'TEXT'),
            ('FAX', 'TEXT'),
            ('TERMIN', 'DATETIME'),
            ('VORLAGE', 'TEXT'),
            ('PPZ', 'TEXT'),
            ('GEPLZ', 'TEXT'),
            ('GEORT', 'TEXT'),
            ('NOTIZ', 'TEXT'),
            ('KUNDENNR', 'INT'),
            ('INET', 'TEXT'),
            ('EMAIL', 'TEXT'),
            ('NAME2', 'TEXT'),
            ('FAX2', 'TEXT'),
            ('TEXT3', 'TEXT'),
            ('TEXT4', 'TEXT'),
            ('TELEFON2', 'TEXT'),
            ('TEXT', 'TEXT'),
            ('TEXT5', 'TEXT'),
            ('STRASSE2', 'TEXT'),
            ('PLZ2', 'TEXT'),
            ('ORT2', 'TEXT'),
            ('EMAIL2', 'TEXT'),
            ('TEXT6', 'TEXT'),
            ('DATUM', 'DATETIME'),
            ('DATUM2', 'DATETIME'),
            ('DATUM3', 'DATETIME'),
            ('TEXT2', 'TEXT'),
            ('TEXT7', 'TEXT'),
            ('TEXT8', 'TEXT'),
            ('ZAHL2', 'REAL'),
            ('ZAHL3', 'REAL'),
            ('ZAHL4', 'REAL'),
            ('ZAHL5', 'REAL'),
            ('ZAHL6', 'REAL'),
            ('TEXT9', 'TEXT'),
            ('ANREDE2', 'TEXT'),
            ('FIRMA', 'TEXT'),
            ('INET2', 'TEXT'),
            ('TEXT10', 'TEXT'),
            ('DIREKT', 'TEXT'),
        ])

db_import("Dokumente", [
                ('ID',        'INT'),
                ('OBERID',    'INT'),
                ('PERSE',     'TEXT'),
                ('DERFASST',  'DATETIME'),
                ('ZERFASST',  'DATETIME'),
                ('KLASSE',    'TEXT'),
                ('DATEINAME', 'TEXT'),
                ('NOTIZ',     'TEXT'),
        ])

db_import("FeldInfos", [
            ('TabName',       'TEXT'),
            ('TabNameNativ',  'TEXT'),
            ('FeldName',      'TEXT'),
            ('FeldNameNativ', 'TEXT'),
            ('SystemFeld',    'BOOLEAN'),
            ('SymbolFeld',    'BOOLEAN'),
        ])

db_import("Kontakte", [
                ('ID',        'INT'),
                ('OBERID',    'INT'),
                ('PERSE',     'TEXT'),
                ('DERFASST',  'DATETIME'),
                ('ZERFASST',  'DATETIME'),
                ('ART',       'TEXT'),
                ('ISTPRIVAT', 'BOOLEAN'),
                ('NOTIZ',     'TEXT'),
        ])

#Script ist leer

db_import("Stichworter", [
                ('ID',          'INT'),
                ('OBERID',      'INT'),
                ('StichwortId', 'INT'),
        ])

db_import("Stichwortliste", [
                ('ID',        'INT'),
                ('Stichwort', 'TEXT'),
        ])

# Symbolliste enthaelt nur binblobs

db_import("Telefonnummern", [
                ('AEDatensatzId',       'INT'),
                ('AEDatensatzFeldName', 'TEXT'),
                ('AETelefon',           'TEXT'),
        ])

db_import("Zusatzdaten", [
            ('ID', 'INT'),
            ('OBERID', 'INT'),
            ('PERSE', 'TEXT'),
            ('DERFASST', 'DATETIME'),
            ('ZERFASST', 'DATETIME'),
            ('PERSG', 'TEXT'),
            ('DGEAENDERT', 'TEXT'),
            ('ZGEAENDERT', 'TEXT'),
        ])

db.close()
