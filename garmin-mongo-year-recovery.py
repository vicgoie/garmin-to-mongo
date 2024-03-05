#!/usr/bin/env python3
import logging
from datetime import datetime
import json
from dotenv import load_dotenv
import os
from pymongo import MongoClient
import calendar

from garminconnect import (
    Garmin,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError,
    GarminConnectAuthenticationError,
)

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

garmin_user = os.getenv("GARMIN_USER")
garmin_pass = os.getenv("GARMIN_PASS")

# Establecer la conexión con MongoDB
mongodb_uri = os.getenv("MONGODB_URI_SERVER")
cliente_mongo = MongoClient(mongodb_uri)
base_datos = cliente_mongo.get_database("HealthDB")
coleccion = base_datos["health_stats"]
coleccion_activities = base_datos["activities"]

# Configure debug logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

try:
    # API

    ## Initialize Garmin api with your credentials
    api = Garmin(garmin_user, garmin_pass)

    ## Login to Garmin Connect portal
    api.login()

    # USER INFO

    # USER STATISTIC SUMMARIES

    # Año específico que deseas procesar
    year = 2024

    # Iterar sobre todos los meses del año
    for month in range(1, 13):
        # Obtener el número de días en el mes
        num_days = calendar.monthrange(year, month)[1]
        print(f"Procesando el mes {month} del año {year}, que tiene {num_days} días.")
        
        # Iterar sobre todos los días del mes
        for day in range(1, num_days + 1):  # Sumamos 1 para incluir el último día
            # Crear la fecha para el día actual
            date = datetime(year, month, day)
            date_str = date.strftime("%Y-%m-%d")  # Formato YYYY-MM-DD
            
            try:
                # Obtener estadísticas para el día actual
                stats_data = api.get_stats(date_str)
                
                # Verificar si existen estadísticas y si tienen un UUID válido
                if stats_data and stats_data.get("uuid"):
                    # Insertar las estadísticas en la colección health_stats
                    coleccion.insert_one(stats_data)
                    print(f"Estadísticas insertadas correctamente para el día {date_str}.")
                else:
                    print(f"Las estadísticas para el día {date_str} son nulas o no tienen un UUID válido.")
            
            except (GarminConnectConnectionError, GarminConnectAuthenticationError,
                    GarminConnectTooManyRequestsError) as err:
                print(f"Error al obtener estadísticas para el día {date_str}: {err}")
        
        # Obtener todas las actividades
        all_activities = api.get_activities(0,366)
        
        # Filtrar las actividades para incluir solo las del mes actual
        activities = [actividad for actividad in all_activities if
                    datetime.strptime(actividad['startTimeLocal'], '%Y-%m-%d %H:%M:%S').year == year and
                    datetime.strptime(actividad['startTimeLocal'], '%Y-%m-%d %H:%M:%S').month == month]
        
        # Iterar sobre todas las actividades del mes
        for actividad in activities:
            try:
                # Obtener la fecha de inicio de la actividad y convertirla a un objeto datetime
                fecha_inicio = datetime.strptime(actividad['startTimeLocal'], '%Y-%m-%d %H:%M:%S')
                
                # Insertar la actividad en la colección de actividades
                coleccion_activities.insert_one(actividad)
                print("Actividad insertada en la base de datos.")
            
            except Exception as e:
                print(f"Error al procesar la actividad: {e}")

 
except (
    GarminConnectConnectionError,
    GarminConnectAuthenticationError,
    GarminConnectTooManyRequestsError,
) as err:
    print("Error:", err)
