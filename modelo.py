import pickle
import numpy as np
import pandas as pd
import datetime
import holidays
from datetime import timedelta
from json import dumps
from httplib2 import Http
import warnings
import schedule
import time
import openpyxl
from openpyxl import Workbook
#import blpapi
#import pdblp

"""
# CONEXION A BLOOMBERG
con = pdblp.BCon()
con.start()

def obtener_volumenes():
    global vol_actual
    global vol_previo
    global vol_momentum

    # Descomenta las siguientes lineas cuando estés listo para conectarte a Bloomberg

    #Obten el nuevo volumen actual
    vol_actual = con.bdp('DTTCAMOU Index', 'PX_LAST')

    # Si estamos en la primera ejecución, recupera el volumen de hace 5 minutos
    if vol_previo == 0:
        #obtain 5 mins ago volume level
        five_min_ago = datetime.datetime.now() - timedelta(minutes=5)
        str_five_min_ago = five_min_ago.strftime("%Y%m%d %H:%M:%S")
        df_vol = con.bdh('DTTCAMOU Index', 'PX_LAST', str_five_min_ago, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), elms=[("periodicitySelection", "MINUTE")])
        # as we want the PX_LAST from 5 minutes ago, which should be the first entry
        vol_previo = df_vol.iloc[0]
    # else, Vol_actual se vuelve vol_previo
    else:
        vol_previo = vol_actual

    # Calcula el momentum como la variación porcentual
    # si vol_previo es 0, hacemos momentum = 0 para evitar división por cero
    vol_momentum = (vol_actual - vol_previo) / vol_previo if vol_previo != 0 else 0
"""



warnings.filterwarnings("ignore")

# PARA CREAR LIBRO EN CASO QUE NO EXISTA
#wb = Workbook()
# Grabar la hoja activa
#ws = wb.active
# Crear el archivo resultados.xlsx
#wb.save(filename = 'resultados.xlsx')

def envio_a_hangouts(mensaje):
    def main():
   
        url = "https://chat.googleapis.com/v1/spaces/AAAAjulzrIE/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=nRynkooRyts2wUdxd0DPeivMKoNHNbxVWVMTXaMX_s8"
        app_message = {"text": mensaje}
        message_headers = {"Content-Type": "application/json; charset=UTF-8"}
        http_obj = Http()
        response = http_obj.request(
            uri=url,
            method="POST",
            headers=message_headers,
            body=dumps(app_message),
        )

        #print(response)
    if __name__ == "__main__":
        main()

# INSERT OUTPUT EXCEL

def insert_row(file_name, data_row):
    workbook = openpyxl.load_workbook(filename=file_name)
    sheet = workbook.active
    sheet.append(data_row)
    workbook.save(file_name)

envio_a_hangouts(f"Proceso iniciado a las {datetime.datetime.now()}")
print(f"Proceso iniciado a las {datetime.datetime.now()}")

#CARGA MODELO

with open('xgboost_model.pkl', 'rb') as file:
    loaded_model = pickle.load(file)

def tarea():
    #obtener_volumenes()
    # USAR STARTIME PARA MEDIR TIEMPO DE PROCESO
    #start_time = datetime.datetime.now()

    # FEATURES 
    fecha = datetime.datetime.today()
    now = datetime.datetime.now()
    chl_holidays = holidays.CountryHoliday('CL', years=[fecha.year])
    us_holidays = holidays.US()


    dia_semana = fecha.weekday()
    previo_feriado_cl = int((fecha + timedelta(days=1)) in chl_holidays)
    post_feriado_cl = int((fecha - timedelta(days=1)) in chl_holidays)
    minutos_dia = 0 if now.hour < 9 else now - datetime.datetime(now.year, now.month, now.day, 9)
    minutos_dia = minutos_dia.total_seconds() / 60
    feriado_us=int(fecha in us_holidays)

    # INPUT PARA EL MODELO


    vector_prueba_col = {'dia_sem': [dia_semana],
        'es_previo_feriado': [previo_feriado_cl],
        'es_post_feriado': [post_feriado_cl],
        'minutos_dia': [minutos_dia],
        'es_feriado_us': [feriado_us],
        'vol_actual': [300],  # vol_actual
        'vol_momentum': [0.15]  # vol_momentum
        }


    vector_prueba=pd.DataFrame(vector_prueba_col)

    # ENVIO 

    envio_a_hangouts(f"{fecha} / VOL ACTUAL: {vector_prueba_col['vol_actual']} / PREDICCION CIERRE: {loaded_model.predict(vector_prueba)}")
    print(f"{fecha} / VOL ACTUAL: {vector_prueba_col['vol_actual']} / PREDICCION CIERRE: {loaded_model.predict(vector_prueba)}")
    insert_row("resultados.xlsx", [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),vector_prueba_col['vol_actual'][0],loaded_model.predict(vector_prueba)[0]])

    # USAR ENDTIME PARA MEDIR TIEMPO DE PROCESO
    #end_time = datetime.datetime.now()
    #print(f"Tarea finalizada. Duración: {end_time - start_time}")
    pass

# Calcula el tiempo para esperar hasta que los minutos sean múltiplos de cinco.
esperar = 300 - datetime.datetime.now().timestamp() % 300
print(f"Esperar: {esperar} segundos")
# Espera hasta el próximo múltiplo de 5 minutos
time.sleep(esperar)
# Luego, programa la tarea para ejecutarse cada 5 minutos
while True:
    now = datetime.datetime.now()
    if now.hour == 13 and now.minute == 45:
        break
    tarea()
    time.sleep(300)



