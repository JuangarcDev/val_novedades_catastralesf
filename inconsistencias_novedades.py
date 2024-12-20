"""
DOCUMENTACIÓN DEL SCRIPT VALIDACIÓN INCONSISTENCIAS NOVEDADES

Este script identifica registros faltantes al comparar los datos de un archivo XML con los de una base de datos PostgreSQL. 
Se utiliza para verificar qué trámites de una tabla específica (tramite) de la base de datos(Novedades_Tramites) no están presentes en el archivo XML, 
basándose en el ID del tramite y el número de resolución.

HERRAMIENTAS UTILIZADAS:
- PostgreSQL: Base de datos utilizada para consultar los registros.
- psycopg2: Librería para conectarse y ejecutar consultas en PostgreSQL.
- xml.etree.ElementTree: Módulo para procesar y extraer datos de archivos XML.
- decouple: Librería para manejar credenciales y configuración sensible desde un archivo `.env`.

FUNCIONAMIENTO:
1. Conexión a la base de datos PostgreSQL.
2. Consulta de registros desde una tabla específica, según criterios de municipio, fechas, y campo "número de resolución".
3. Extracción de datos relevantes del archivo XML.
4. Comparación de los datos de la base de datos con los del archivo XML para identificar registros faltantes.
5. Generación de un archivo de log con el flujo detallado de ejecución, resultados, y una consulta SQL para analizar los registros faltantes directamente en la base de datos.

PARAMETROS/INPUTS:
1. Credenciales de la base de datos PostgreSQL (host, nombre de la base de datos, usuario, contraseña, puerto).
2. Ubicación del archivo XML a analizar.
3. Nombre de la tabla de la base de datos a evaluar.
4. Código del municipio para filtrar registros en la base de datos.
5. Fecha inicial y final para acotar el rango de los registros evaluados.

OUTPUTS:
1. Archivo de log (`log.txt`) que incluye:
   - Inicio del script.
   - Estado de la conexión a la base de datos.
   - Detalles de las consultas realizadas, incluyendo cantidad total y los primeros 10 registros.
   - Identificación de registros faltantes (cantidad total y los primeros 10).
   - Consulta SQL generada para ejecutar en pgAdmin y analizar los registros faltantes.
   - Finalización del script.
"""
import psycopg2
import xml.etree.ElementTree as ET
from decouple import config

log_file = "log.txt"

def log_message(message):
    """
    Escribe un mensaje en el archivo de log y lo imprime en la consola.
    """
    with open(log_file, "a") as log_filee:
        log_filee.write(message + "\n")
    print(message)

def connect_to_db():
    """
    Función que sirve para conectar con la Base de Datos de PostgreSQL
    """
    try:
        conn = psycopg2.connect(
            host=config('DB_HOST'),
            database=config('DB_NAME'),
            user=config('DB_USER'),
            password=config('DB_PASSWORD'),
            port=config('DB_PORT')
        )
        log_message("Conexión exitosa a la Base de Datos PostgreSQL.")
        return conn
    except Exception as e:
        log_message(f"Error al conectar con la Base de Datos PostgreSQL: {e}")
        return None
    
def get_table_data(conn, table_name, town, date_i, date_f):
    """
    Obtiene los datos de id, núnero de resolución de una tabla especifica (tramite) de la DB PostgreSQL, para un municipio y rango de fechas descrito
    """
    try:
        cursor = conn.cursor()
        query = f"""
        SELECT id, resolution_number 
        FROM {table_name} 
        WHERE town = '{town}'
        AND estado_actual_fecha_inicio BETWEEN '{date_i}' AND '{date_f}'
        AND resolution_number IS NOT NULL
        ORDER BY resolution_number ASC
        """
        cursor.execute(query)
        result = cursor.fetchall()
        log_message(f"Consulta realizada a la tabla '{table_name}' con {len(result)} registros obtenidos.")
        if result:
            log_message("Primeros 10 registros de la tabla:")
            for row in result[:10]:
                log_message(f"ID TRAMITE: {row[0]}, NUMERO DE RESOLUCION: {row[1]}")
        return {row[0]: row[1] for row in result}  # Diccionario {id tramite: num resolución}
    except Exception as e:
        log_message(f"Error consultando la tabla {table_name}: {e}")
        return {}
    
def parse_xml(file_path):
    """
    Lee y procesa el archivo XML. Extrae un diccionario radicado: resolucion
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        # Extrae los datos relevantes, ajustable según la estructura de XML
        xml_data = {}

        # Buscar todos los elementos <mutacion_rectificacion>
        for record in root.findall('./mutacion_rectificacion'):
            record_id = record.find('radicado').text  # ID obtenido de <radicado>
            record_value = record.find('resolucion').text  # Valor de <resolucion>
            if record_id and record_value:  # Solo agregar si ambos están presentes
                xml_data[record_id] = record_value

        # Mensaje de seguimiento
        log_message(f"Archivo XML '{file_path}' procesado con {len(xml_data)} registros obtenidos.")
        
        # Mostrar los 10 primeros registros        
        if xml_data:
            log_message("Primeros 10 registros del XML:")
            for idx, (record_id, record_value) in enumerate(xml_data.items()):
                if idx >= 10:
                    break
                log_message(f"ID: {record_id}, Resolución: {record_value}")

        return xml_data
    except Exception as e:
        log_message(f"Error procesando el archivo XML '{file_path}': {e}")
        return {}
    
def find_missing_records(db_data, xml_data):
    """
    Encuentra los registros presentes en la base de datos pero faltantes en el XML
    basándose en los valores de 'resolution_number' y 'record_value'. Haciendo contraste de registros del diccionario extraido de la DB
    VS el diccionario extraido del XML
    """
    # Convertir los valores de xml_data a un conjunto para comparaciones eficientes
    xml_values = set(xml_data.values())
    
    # Encontrar los registros en db_data cuyos valores no están en xml_data
    missing_records = {key: value for key, value in db_data.items() if value not in xml_values}
    
    # Logging de los resultados
    log_message(f"Se encontraron {len(missing_records)} registros faltantes.")
    if missing_records:
        log_message("Primeros 10 registros faltantes:")
        for idx, (record_id, record_value) in enumerate(missing_records.items()):
            if idx >= 10:
                break
            log_message(f"ID: {record_id}, Resolución: {record_value}")
    
    return missing_records

def main():
    # Sobrescribir el archivo de log al inicio
    with open(log_file, "w") as log_filee:
        log_filee.write("Inicio del script.\n")

    db_connection = connect_to_db()
    if not db_connection:
        log_message("Conexión fallida. Terminando el script.")
        return
    
    try:
        # PARAMETROS PARA CONSULTA DE DATOS EN LA DB DE POSTGRESQL
        db_table_name = 'data.tramite' # CAMBIAR NOMBRE DE LA TABLA, DE SER NECESARIO
        db_town ='25168'
        db_date_i = '2024-11-01'
        db_date_f = '2024-12-12'
        db_data = get_table_data(db_connection, db_table_name, db_town, db_date_i, db_date_f)

        # PROCESAR EL ARCHIVO XML
        xml_file_path =  r"C:\ACC\Novedades_Catastrales\INSUMOS\XML\Registro_novedades_25168.xml"  # Ruta del archivo XML
        xml_data = parse_xml(xml_file_path)

        # IDENTIFICAR LOS REGISTROS FALTANTES (PRESENTES EN LA DB Y FALTANTES EN EL XML)
        missing_records = find_missing_records(db_data, xml_data)

        log_message("Proceso completado. Los registros faltantes se han identificado: ")
        for record_id, record_value in missing_records.items():
            log_message(f"ID TRAMITE: {record_id}, NUMERO RESOLUCION: {record_value}")
    
        # Sentencia SQL final para copiar y pegar en la DB
        if missing_records:
            sql_query = f"""
            SELECT * 
            FROM {db_table_name}
            WHERE town = '{db_town}'
              AND estado_actual_fecha_inicio BETWEEN '{db_date_i}' AND '{db_date_f}'
              AND resolution_number IN ({', '.join(f"'{value}'" for value in missing_records.values())})
            ORDER BY resolution_date ASC;
            """
            log_message("Consulta SQL para insertar en la DB:")
            log_message(sql_query)
                
    finally:
        db_connection.close()
        log_message("Conexión con la Base de Datos cerrada.")
    log_message("Fin del script.")

if __name__ == "__main__":
    main()