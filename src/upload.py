import MySQLdb,os

path='testData'
absPath = os.path.abspath(path)
print absPath

conn = MySQLdb.connect(host='ngrds1.cbg5ljupg65a.us-east-1.rds.amazonaws.com',
                          user='awsuser',
                          passwd='compudev',
                          db='mydb')

db_cursor = conn.cursor()
#query = "LOAD DATA LOCAL INFILE 'msft.csv' INTO TABLE mydb.stocks_daily_load FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (symbol, date, open, high, low, close, volume, adj_close)"
query = "LOAD DATA LOCAL INFILE 'nasdaqlisted.txt' INTO TABLE mydb.symbol_master FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' (symbol, security_name, market_category, test_issue, financial_status, round_lot_size)"

db_cursor.execute(query)
conn.commit()
