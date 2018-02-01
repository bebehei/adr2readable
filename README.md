# adr2readable

Export Cobra CRM database to another CRM. This is superold and I'm glad to actually extract it via software.

This is mostly used in Germany, therefore no english explanation.


```
./01-adr2csv.sh <Datenbank.adr> <output>
./02-csv2sqlite.py

```

Ergibt eine vollständige Kopie der Access-Datenbank in sqlite3. 

Das Ziel-CRM steht noch nicht fest. Wir werden sehen, wie die Reise weitergeht.
