import psycopg2
from pymongo import MongoClient
import pymongo
import json

def conex_postgres():
    conn_string = "host='localhost' dbname='BaseTwitter' user='postgres' password='qwer$%'"
    conn = psycopg2.connect(conn_string)
    print("****")
 
    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
    return cursor

def get_ids():
    cur = conex_postgres()
    sql_string = """SELECT id from tuits ;"""
    print(sql_string)
    cur.execute(sql_string)
    ids = cur.fetchall()
    return ids 
        
def construye():
    cur = conex_postgres()
    rs = get_ids()
    
    for id in rs:
        cur.execute("select row_to_json(row) from (select * from tuits where id="+str(id[0])+")row;")
        tuit = cur.fetchall()
        t = json.loads(tuit[0][0])
        cur.execute("select row_to_json(row) from (select latitud,longitud from coordenadas where id_tuit="+str(id[0])+")row;")
        cord = cur.fetchall()
        if not(not cord):
            c = json.loads(cord[0][0])
            t['coordenadas'] = c
        else:
             t['coordenadas'] = cord
        cur.execute("select row_to_json(row) from (select latitud,longitud from boundings where id="+str(id[0])+")row;")
        bounding = cur.fetchall()
        if not(not bounding):
            c = json.loads(bounding[0][0])
            t['bound'] = c
        else:
             t['bound'] = bounding
        cur.execute("select row_to_json(row) from (select text,ind_inicial,ind_final from hashtags where id_tuits="+str(id[0])+")row;")
        hash = cur.fetchall()
        if not(not hash):
            c = json.loads(hash[0][0])
            t['hashtags'] = c
        else:
             t['hashtags'] = hash
        cur.execute("select row_to_json(row) from (select full_name,country,place_type,id,name from place where id_tuit="+str(id[0])+")row;")
        place = cur.fetchall()
        if not(not place):
            c = json.loads(place[0][0])
            t['place'] =c 
        else:
             t['place'] = place
        cur.execute("select row_to_json(row) from (select ind_inicial,ind_final,screen_name, name from user_mentions where id_tuits="+str(id[0])+")row;")
        um = cur.fetchall()
        if not(not um):
            c = json.loads(um[0][0])
            t['user_mentions'] = c
        else:
             t['user_mentions'] = um
        
        cur.execute("select row_to_json(row) from (select * from usuarios where id_tuit="+str(id[0])+")row;")
        u = cur.fetchall()
        if not(not u):
            c = json.loads(u[0][0])
            t['users'] = c
        else:
             t['users'] = u
             
        insertMongo(t)
def insertMongo(objeto):
    try:
        client = MongoClient()
        db = client.Twitter#Twitter es el nombre de la base
        
        db.tuits.insert(objeto)
        print "----"
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to server: %s" % e
    #exec insert#'''


if __name__ =="__main__":
     construye()
