import csv
import json
import os.path
import xml.etree.ElementTree as ET

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class MissionTestUpdater:
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    hoja_todos = "Todos"
    hoja_con_sensor = "Con Sensor"
    hoja_sin_sensor = "Sin Sensor"
    hoja_terapeutas = "Terapeutas"
    hoja_ics = "ICS"
    rango_hojas_encuesta = "A1:AK20"

    rango_ics_con_sensor = "A1:K8"
    rango_speed_con_sensor = "A9:K16"
    rango_interval_con_sensor = "A17:K24"
    rango_range_con_sensor = "A25:K32"

    rango_ics_sin_sensor = "L1:V8"
    rango_speed_sin_sensor = "L9:V16"
    rango_interval_sin_sensor = "L17:V24"
    rango_range_sin_sensor = "L25:V32"

    def __init__(self,
                 spreadsheet_id,
                 cvs_path,
                 xml_con_sensor_path,
                 xml_sin_sensor_path):
        self.spreadsheet_id = spreadsheet_id
        self.cvs_path = cvs_path
        self.xml_con_sensor_path = xml_con_sensor_path
        self.xml_sin_sensor_path = xml_sin_sensor_path

        self.creds = None
        self.cvs_data = []
        self.con_sensor_data = []
        self.sin_sensor_data = []
        self.terapeutas_data = []

        self._get_credentials()
        self._get_csv_data()
        self.num_data_in_spreadsheet = self.get_number_of_rows()
        self.data_to_update = self.cvs_data[self.num_data_in_spreadsheet:]
        self._separate_sensor_users()
        self._get_terapeutas_row()

        num_ics_con_sensor = self.get_number_of_rows(self.hoja_ics, self.rango_ics_con_sensor)
        num_ics_sin_sensor = self.get_number_of_rows(self.hoja_ics, self.rango_ics_sin_sensor)
        self.data_con_sensor = self._get_ics(self.xml_con_sensor_path)
        self.data_sin_sensor = self._get_ics(self.xml_sin_sensor_path)

    def _get_credentials(self):
        self.creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists("token.json"):
            self.creds = Credentials.from_authorized_user_file("token.json", self.scopes)
        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", self.scopes
                )
                self.creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open("token.json", "w") as token:
                token.write(self.creds.to_json())

    def _get_ics(self, dir_path):
        ics = []
        speed = []
        interval = []
        rango = []
        for i in range(1, len(os.listdir(dir_path)) + 1):
            ic, s, i, r = self._get_last_ic(os.path.join(dir_path, f"1_Data{i:02d}.xml"))
            ics.append(ic)
            speed.append(s)
            interval.append(i)
            rango.append(r)
        return [ics, speed, interval, rango]

    def _get_last_ic(self, file_path, num_rondas=10):
        """Obtiene los ics de las ultimas rondas"""
        tree = ET.parse(file_path)
        root = tree.getroot()

        ultimas_partidas = root.find("HistorialPartidas")[:num_rondas]

        ics = [float(p.find("dificultad").text) for p in ultimas_partidas]
        ics.reverse()
        speeds = [float(p.find("velocidad").text) for p in ultimas_partidas]
        speeds.reverse()
        interval = [float(p.find("intervalo").text) for p in ultimas_partidas]
        interval.reverse()
        range = [float(p.find("rango").text) for p in ultimas_partidas]
        range.reverse()

        return ics, speeds, interval, range

    def _get_csv_data(self):
        with open(self.cvs_path, "r", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file, delimiter=",")
            self.cvs_data = [[x for x in row] for row in reader]

    def _separate_sensor_users(self):
        self.con_sensor_data = [row for row in self.data_to_update if "-1" not in row[17]]
        self.sin_sensor_data = [row for row in self.data_to_update if "-1" in row[17]]

    def _get_terapeutas_row(self):
        self.terapeutas_data = [row for row in self.data_to_update if "-1" not in row[30]]

    def append_to_spreadsheets(self, table_range, body):
        """
        Agrega nuevos datos al final de la tabla que esta contenida en el table_range
        :param table_range: Rango donde se encuentra la tabla al final de la cual se insertaran nuevos datos
        :param body: Lista de listas con los datos que se van a agregar
        """
        print('holis')
        try:
            service = build("sheets", "v4", credentials=self.creds)

            # Call the Sheets API
            sheet = service.spreadsheets()
            result = (
                sheet.values()
                .append(
                  spreadsheetId=self.spreadsheet_id,
                  range=table_range,
                  valueInputOption="USER_ENTERED",
                  body=body
                )
                .execute()
            )

            print(f"Rango de celdas actualizadas: {result.get('tableRange')}")
        except HttpError as err:
            print(err)

    def batch_update_to_spreadsheets(self, body):
        """
        Agrega nuevos datos en la hoja de calculo en el batch indicado
        :param body: Diccionario con los valores y rangos a subir.
        Ver https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchUpdate?hl=en
        """
        try:
            service = build("sheets", "v4", credentials=self.creds)

            # Call the Sheets API
            sheet = service.spreadsheets()
            result = (
                sheet.values()
                .batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=body
                )
                .execute()
            )

            print(f"Numero de celdas actualizadas: {result.get('totalUpdatedCells')}")
        except HttpError as err:
            print(err)

    def get_number_of_rows(self, hoja=hoja_todos, rango=rango_hojas_encuesta):
        """Devuelve el numero de pruebas en spreadsheet"""
        try:
            service = build("sheets", "v4", credentials=self.creds)

            # Call the Sheets API
            sheet = service.spreadsheets()
            result = (
                sheet.values()
                .get(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"{hoja}!{rango}"
                )
                .execute()
            )

            print(f"Numero de filas en tabla: {len(result.get('values', []))}")
            return len(result.get("values", []))
        except HttpError as err:
            print(err)

    def update_encuesta_data(self):
        if not self.data_to_update:
            print("No hay valores por acutalizar")
            return

        body_todos = {"values": self.data_to_update}
        body_con_sensor = {"values": self.con_sensor_data}
        body_sin_sensor = {"values": self.sin_sensor_data}
        body_terapeutas = {"values": self.terapeutas_data}

        self.append_to_spreadsheets(f"{self.hoja_todos}!{self.rango_hojas_encuesta}", body_todos)
        self.append_to_spreadsheets(f"{self.hoja_con_sensor}!{self.rango_hojas_encuesta}", body_con_sensor)
        self.append_to_spreadsheets(f"{self.hoja_sin_sensor}!{self.rango_hojas_encuesta}", body_sin_sensor)
        self.append_to_spreadsheets(f"{self.hoja_terapeutas}!{self.rango_hojas_encuesta}", body_terapeutas)

    def update_ics(self):
        print(self.data_con_sensor)
        if self.data_con_sensor:
            body_ics = {"values": self.data_con_sensor[0]}
            body_speed = {"values": self.data_con_sensor[1]}
            body_interval = {"values": self.data_con_sensor[2]}
            body_range = {"values": self.data_con_sensor[3]}

            self.append_to_spreadsheets(f"{self.hoja_ics}!{self.rango_ics_con_sensor}", body_ics)
            self.append_to_spreadsheets(f"{self.hoja_ics}!{self.rango_speed_con_sensor}", body_speed)
            self.append_to_spreadsheets(f"{self.hoja_ics}!{self.rango_interval_con_sensor}", body_interval)
            self.append_to_spreadsheets(f"{self.hoja_ics}!{self.rango_range_con_sensor}", body_range)

        if self.data_sin_sensor:
            body_ics = {"values": self.data_sin_sensor[0]}
            body_speed = {"values": self.data_sin_sensor[1]}
            body_interval = {"values": self.data_sin_sensor[2]}
            body_range = {"values": self.data_sin_sensor[3]}

            self.append_to_spreadsheets(f"{self.hoja_ics}!{self.rango_ics_sin_sensor}", body_ics)
            self.append_to_spreadsheets(f"{self.hoja_ics}!{self.rango_speed_sin_sensor}", body_speed)
            self.append_to_spreadsheets(f"{self.hoja_ics}!{self.rango_interval_sin_sensor}", body_interval)
            self.append_to_spreadsheets(f"{self.hoja_ics}!{self.rango_range_sin_sensor}", body_range)


def main():
    with open("./spreadsheet-data.json", "r") as json_file:
        json_data = json.loads(json_file.read())

    mision_test_updater = MissionTestUpdater(json_data["spreadsheet_id"],
                                             json_data["cvs_path"],
                                             json_data["xml_con_sensor_path"],
                                             json_data["xml_sin_sensor_path"])

    #mision_test_updater.update_encuesta_data()
    mision_test_updater.update_ics()
    #mision_test_updater._get_last_ic(os.path.join(mision_test_updater.xml_sin_sensor_path, f"1_Data01.xml"))


if __name__ == "__main__":
    main()
