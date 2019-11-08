import cx_Oracle as co

try:
    conn = co.connect('WIPREAD/Password123@sinlsd108.ap.medtronic.com')
    cur = conn.cursor()

    sqlquery = 'select count(*) from mdt_mcmas_confirmation_buffer'
    data_table = cur.execute(sqlquery)
    for row in data_table:
        print(row[0])
except co.DatabaseError as e:    
    print('There is a problem with Oracle', e)
# finally:
#     if cur:
#         cur.close()
#     if conn:
#         conn.close()