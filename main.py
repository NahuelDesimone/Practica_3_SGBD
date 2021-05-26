import csv
import psycopg2
from configparser import ConfigParser
import numpy as np
import matplotlib.pyplot as plt
from shapely.geometry import Point
from geopandas import GeoSeries, GeoDataFrame
import pandas as pd

def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def abrirBaseDeDatos():
    conn = None
    params = config()
            # connect to the PostgreSQL database
    conn = psycopg2.connect(**params)
    return conn

def cargar(sql):

    conn = None
    try:
        conn = abrirBaseDeDatos()
            # create a new cursor
        cur = conn.cursor()
            # execute the INSERT statement
        cur.execute(sql)

        row = cur.fetchone()
        diccionario = {}
        while row is not None:
            clave = row[0]
            valor = row[1] 

            diccionario[clave] = valor
            row = cur.fetchone()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()

    return diccionario


def procesarArchivoCsv():
    sql = "select code2,code from country"
    archivoCSV = open('/home/nahueldesimone/Descargas/practica3/top-1m.csv')
    datos = csv.reader(archivoCSV)
    dic = cargar(sql)
    conn = abrirBaseDeDatos()

    for elem in datos:
        id = elem[0]
        dominio = elem[1]
    
        lista = dominio.split('.')

        entidad = lista[0]

        tipoEntidad = ""

        countryCode = ""

        codigoPais = ""

        ultimoValor = lista.pop()

        if(dic.get(ultimoValor.upper()) != None):
            countryCode = dic[ultimoValor.upper()]
            codigoPais = ultimoValor.upper()

        if (len(lista) == 3 and countryCode == ""):
            codigoPais = "US"
            countryCode = dic[codigoPais]
            tipoEntidad = ultimoValor

        if (len(lista) == 2 and countryCode == ""):
            codigoPais = "US"
            countryCode = dic[codigoPais]
            tipoEntidad = ultimoValor

        if(len(lista) == 2 and tipoEntidad == ""):
            tipoEntidad =lista[1]

        if(len(lista) == 1 and countryCode == ""):
            codigoPais = "US"
            countryCode = dic[codigoPais]
            tipoEntidad = ultimoValor

        if(countryCode == ""):
            print(entidad)

        #insert_sitio(int(id),entidad,tipoEntidad,codigoPais,countryCode, conn)

    if conn is not None:
        conn.close()

	
def insert_sitio(id,entidad, tipoEntidad,codigoPais,countryCode, conn):

    sql = "INSERT INTO sitio (id,entidad,tipo_entidad,pais,countrycode) VALUES(%s,%s,%s,%s,%s)"
    try:
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (id,entidad, tipoEntidad,codigoPais,countryCode))

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)



def graficoPoblacion ():
    #QUERY
    sql = "select name,population from country order by name"
    dicPoblacion = cargar(sql)
    world = GeoDataFrame.from_file('ne_10m_admin_0_countries.shp').sort_values('NAME').set_index('NAME')
 
    listaPaisesDf = world.index.tolist()
    listaPaisesDic = list(dicPoblacion.keys())

    for pais in listaPaisesDf:
        if (pais in listaPaisesDic):
            world.at[pais,'POPULATION'] = dicPoblacion[pais]
        else:
            world.at[pais,'POPULATION'] = 0



    # para ver los colormap, ejecutar colors.py
    world.plot(column='POPULATION', colormap='Reds', alpha=1, categorical=False, legend=True, axes=None)
    #orld.plot(column='prueba', colormap='binary', alpha=0.5, categorical=False, legend=False, axes=None)
    plt.show()

def graficoPBI ():
    #QUERY
    sql = "select name,gnp from country order by name"
    dicPBI = cargar(sql)
    world = GeoDataFrame.from_file('ne_10m_admin_0_countries.shp').sort_values('NAME').set_index('NAME')
 
    listaPaisesDf = world.index.tolist()
    listaPaisesDic = list(dicPBI.keys())

    for pais in listaPaisesDf:
        if (pais in listaPaisesDic):
            world.at[pais,'PBI'] = dicPBI[pais]
        else:
            world.at[pais,'PBI'] = 0

    # para ver los colormap, ejecutar colors.py
    world.plot(column='PBI', colormap='Greens', alpha=1, categorical=False, legend=False, axes=None)
    plt.show()

def graficoSitios():
    #QUERY
    sql = "select c.name,count(*) from sitio s inner join country c on s.countrycode = c.code group by c.name"
    dicSitios = cargar(sql)
    world = GeoDataFrame.from_file('ne_10m_admin_0_countries.shp').sort_values('NAME').set_index('NAME')
 
    listaPaisesDf = world.index.tolist()
    listaPaisesDic = list(dicSitios.keys())

    for pais in listaPaisesDf:
        if (pais in listaPaisesDic):
            world.at[pais,'SITIOS'] = dicSitios[pais]
        else:
            world.at[pais,'SITIOS'] = 0

    # para ver los colormap, ejecutar colors.py
    world.plot(column='SITIOS', colormap='Reds', alpha=1, categorical=False, legend=True, axes=None)
    plt.show()


#procesarArchivoCsv()
graficoPoblacion()
graficoPBI()
graficoSitios()