#!/usr/bin/env python3
import logging
import datetime
import json
from dotenv import load_dotenv
import os
from pymongo import MongoClient
import paho.mqtt.client as mqtt

from garminconnect import (
    Garmin,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError,
    GarminConnectAuthenticationError,
)
load_dotenv()

garmin_user = os.getenv("GARMIN_USER")
garmin_pass = os.getenv("GARMIN_PASS")
mongodb_uri = os.getenv("MONGODB_URI_SERVER")
cliente_mongo = MongoClient(mongodb_uri)
base_datos = cliente_mongo.get_database("HealthDB")
coleccion_health_stats = base_datos["health_stats"]
coleccion_activities = base_datos["activities"]

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

BROKER_HOST = os.getenv("BROKER_HOST")
BROKER_PORT = int(os.getenv("BROKER_PORT"))
BROKER_USER = os.getenv("BROKER_USER")
BROKER_PASS = os.getenv("BROKER_PASS")
BROKER_TOPIC_GARMIN = os.getenv("TOPIC_GARMIN")
BROKER_TOPIC_STATS = os.getenv("TOPIC_GARMIN_STATS")
BROKER_TOPIC_ACTIVITY = os.getenv("TOPIC_GARMIN_ACTIVITY")
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.username_pw_set(BROKER_USER, BROKER_PASS)
mqtt_client.connect(BROKER_HOST, BROKER_PORT)

def send_mqtt_message_garmin_stats(message):
    mqtt_client.publish(BROKER_TOPIC_STATS, message)

def send_mqtt_message_garmin_activity(message):
    mqtt_client.publish(BROKER_TOPIC_ACTIVITY, message)

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
#print(today)

def main():
    try:
        api = Garmin(garmin_user, garmin_pass)
        api.login()

        try:
            stats_data = api.get_stats(yesterday)
            if stats_data["uuid"] is not None:
                if not coleccion_health_stats.find_one({"uuid": stats_data["uuid"]}):
                    # El campo "uuid" no existe, por lo que puedes insertar el documento
                    coleccion_health_stats.insert_one(stats_data)
                    print("Datos insertados correctamente en la colección health_stats.")
                    send_mqtt_message_garmin_stats("0")
                else:
                    print("El campo 'uuid' ya existe en la colección. No se realizará la inserción.")
                    send_mqtt_message_garmin_stats("2")
            else:
                print("Este día no existe")
                send_mqtt_message_garmin_stats("3")

        except (
            GarminConnectConnectionError,
            GarminConnectAuthenticationError,
            GarminConnectTooManyRequestsError,
        ) as err:
            print(f"Error al obtener datos para el día {yesterday}: {err}")
            send_mqtt_message_garmin_stats("1")
        
        all_activities = api.get_activities(0, 2)  # Obtener hasta 2 actividades del día
        all_activities.reverse()

        for actividad in all_activities:
            # Verificar si el campo "activityId" no existe en la colección
            if not coleccion_activities.find_one({"activityId": actividad["activityId"]}):
                try:
                    # El campo "activityId" no existe, por lo que puedes insertar la actividad
                    coleccion_activities.insert_one(actividad)
                    print("Datos insertados correctamente en la colección activities.")
                    send_mqtt_message_garmin_activity("0")

                except (
                    GarminConnectConnectionError,
                    GarminConnectAuthenticationError,
                    GarminConnectTooManyRequestsError,
                ) as err:
                    print(f"Error al insertar datos para la actividad: {err}")
                    send_mqtt_message_garmin_activity("1")

            else:
                print("El campo 'activityId' ya existe en la colección. No se realizará la inserción.")
                send_mqtt_message_garmin_activity("2")

    
    except (
            GarminConnectConnectionError,
            GarminConnectAuthenticationError,
            GarminConnectTooManyRequestsError,
        ) as err:
        logger.error("Error occurred during Garmin Connect communication: %s", err)
        send_mqtt_message_garmin_activity("1")
    finally:
        # Cerrar la conexión a la base de datos
        cliente_mongo.close()
        # Cerrar la conexión MQTT
        mqtt_client.disconnect()

if __name__ == "__main__":
    main()