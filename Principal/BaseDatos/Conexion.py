import psycopg2
from pymongo import MongoClient


def conex_postgres():
    conn_string = "host='localhost' dbname='basename' user='postgres' password='***"
    conn = psycopg2.connect(conn_string)
 
    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
    return cursor

def selection():
    cur = conex_postgres()
    sql_string = """SELECT row_to_json(t)FROM (SELECT * FROM tuits t, boundings b , coordenadas c,hashtags h,place p, user_mentions um,usuarios u
    WHERE t.id=b.id_tuit and t.id=c.id_tuit and t.id=h.id_tuits and t.id=um.id_tuits and t.id=u.id_tuit and t.id=p.id_tuit)t;"""
    cur.execute(sql_string)
    rows = cur.fetchall()
    return rows 
        
def insertMongo(self):
    rs = selection()
    
    client = MongoClient()
    db = client.Twitter#Twitter es el nombre de la base
    db.tuits.insert(rs)
    


if __name__ =="__main__":
     conex_postgres()
