import csv
import os.path
import xml.etree.ElementTree as ET

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = "1Aksow5R7eRIe-NP1XGzRAmZzdwz_IfcwiWhPeMekE20"
SAMPLE_RANGE_NAME = "Hoja1!A2:E"

CSV_PATH = "C:\LANR\MisionMeteorica\encuesta_data.txt"
XML_SENSOR = "C:\\LANR\\MisionMeteorica\\Nueva Carpeta\\Con sensor"
XML_SIN_SENSOR = "C:\\LANR\\MisionMeteorica\\Nueva Carpeta\\Sin sensor"

HOJA_TODOS = "Todos"
HOJA_CON_SENSOR = "Con Sensor"
HOJA_SIN_SENSOR = "Sin Sensor"
HOJA_TERAPEUTAS = "Terapeutas"
HOJA_ICS = "ICS"
RANGO = "A1:AK20"


def get_ics(dir_path):
    ics = []
    for i in range(1, len(os.listdir(dir_path)) + 1):
        ics.append(get_last_ic(os.path.join(dir_path, f"1_Data{i:02d}.xml")))
    return ics


def get_last_ic(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    ultimas_partidas = root.find("HistorialPartidas")[:10]

    ics = [float(p.find("dificultad").text) for p in ultimas_partidas]
    ics.reverse()

    return ics


def get_credentials():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return creds


def append_to_spreadsheets(credentials, spreadsheet_id, table_range, body):
    """
    Agrega nuevos datos al final de la tabla que esta contenida en el table_range
    :param credentials: Credenciales de API
    :param spreadsheet_id: ID de la hoja de calculo (en URL)
    :param table_range: Rango donde se encuentra la tabla al final de la cual se insertaran nuevos datos
    :param body: Lista de listas con los datos que se van a agregar
    """
    try:
        service = build("sheets", "v4", credentials=credentials)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .append(
              spreadsheetId=spreadsheet_id,
              range=table_range,
              valueInputOption="USER_ENTERED",
              body=body
            )
            .execute()
        )

        print(f"Rango de celdas actualizadas: {result.get('tableRange')}")
    except HttpError as err:
        print(err)


def update_to_spreadsheets(credentials, spreadsheet_id, update_range, body):
    """
    Agrega nuevos datos en la hoja de calculo en el rango indicado
    :param credentials: Credenciales de API
    :param spreadsheet_id: ID de hoja de calculo (en URL)
    :param update_range: Rango donde se insertaran los datos. Ejemplo: Hoja1!A1:J3
    :param body: Lista de listas con los datos que se insertaran
    """
    try:
        service = build("sheets", "v4", credentials=credentials)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .batchUpdate(
                spreadsheetId=spreadsheet_id,
                # range=update_range,
                # valueInputOption="USER_ENTERED",
                body=body
            )
            .execute()
        )

        print(f"Numero de celdas actualizadas: {result.get('totalUpdatedCells')}")
    except HttpError as err:
        print(err)


def get_number_of_rows(credentials, spreadsheet_id, hoja=HOJA_TODOS, rango=RANGO):
    try:
        service = build("sheets", "v4", credentials=credentials)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=f"{hoja}!{rango}"
            )
            .execute()
        )

        print(f"Numero de filas en tabla: {len(result.get('values', []))}")
        return len(result.get("values", []))
    except HttpError as err:
        print(err)


def get_csv_data(path):
    with open(path, "r", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file, delimiter=",")
        return [[x for x in row] for row in reader]


def separate_sensor_users(values):
    con_sensor = [row for row in values if "-1" not in row[17]]
    sin_sensor = [row for row in values if "-1" in row[17]]
    return con_sensor, sin_sensor


def get_terapeutas_row(values):
    return [row for row in values if "-1" not in row[30]]


def main():
    creds = get_credentials()
    values = get_csv_data(CSV_PATH)
    r = get_number_of_rows(creds, SAMPLE_SPREADSHEET_ID)
    values = values[r:]

    # Datos de IC
    rango_ics_con_sensor = "A1:K15"
    rango_ics_sin_sensor = "L1:Z15"

    num_ics_con_sensor = get_number_of_rows(creds, SAMPLE_SPREADSHEET_ID, HOJA_ICS, rango_ics_con_sensor)
    num_ics_sin_sensor = get_number_of_rows(creds, SAMPLE_SPREADSHEET_ID, HOJA_ICS, rango_ics_sin_sensor)
    ics_con_sensor = get_ics(XML_SENSOR)[num_ics_con_sensor:]
    ics_sin_sensor = get_ics(XML_SIN_SENSOR)[num_ics_sin_sensor:]

    if ics_con_sensor:
        body = {"values": ics_con_sensor}
        append_to_spreadsheets(creds, SAMPLE_SPREADSHEET_ID, f"{HOJA_ICS}!{rango_ics_con_sensor}", body)

    if ics_sin_sensor:
        body = {"values": ics_sin_sensor}
        append_to_spreadsheets(creds, SAMPLE_SPREADSHEET_ID, f"{HOJA_ICS}!{rango_ics_sin_sensor}", body)

    if not values:
        print("No hay valores por actualizar")
        return
    
    con_sensor_values, sin_sensor_values = separate_sensor_users(values)
    terapeutas = get_terapeutas_row(values)

    body_todos = {"values": values}
    body_con_sensor = {"values": con_sensor_values}
    body_sin_sensor = {"values": sin_sensor_values}
    body_terapeutas = {"values": terapeutas}

    append_to_spreadsheets(creds, SAMPLE_SPREADSHEET_ID, f"{HOJA_TODOS}!{RANGO}", body_todos)
    append_to_spreadsheets(creds, SAMPLE_SPREADSHEET_ID, f"{HOJA_CON_SENSOR}!{RANGO}", body_con_sensor)
    append_to_spreadsheets(creds, SAMPLE_SPREADSHEET_ID, f"{HOJA_SIN_SENSOR}!{RANGO}", body_sin_sensor)
    append_to_spreadsheets(creds, SAMPLE_SPREADSHEET_ID, f"{HOJA_TERAPEUTAS}!{RANGO}", body_terapeutas)

    # Datos de IC
    rango_ics_con_sensor = "A1:K20"
    rango_ics_sin_sensor = "L1:Z20"

    num_ics_con_sensor = get_number_of_rows(creds, SAMPLE_SPREADSHEET_ID, HOJA_ICS, rango_ics_con_sensor)
    num_ics_sin_sensor = get_number_of_rows(creds, SAMPLE_SPREADSHEET_ID, HOJA_ICS, rango_ics_sin_sensor)
    ics_con_sensor = get_ics(XML_SENSOR)[num_ics_con_sensor:]
    ics_sin_sensor = get_ics(XML_SIN_SENSOR)[num_ics_sin_sensor:]

    if ics_con_sensor:
        body = {"values": ics_con_sensor}
        append_to_spreadsheets(creds, SAMPLE_SPREADSHEET_ID, f"{HOJA_ICS}!{rango_ics_con_sensor}", body)

    if ics_sin_sensor:
        body = {"values": ics_sin_sensor}
        append_to_spreadsheets(creds, SAMPLE_SPREADSHEET_ID, f"{HOJA_ICS}!{rango_ics_sin_sensor}", body)


if __name__ == "__main__":
    main()
