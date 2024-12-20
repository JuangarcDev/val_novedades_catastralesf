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
    
def get_table_data(conn, table_name):
    """
    Obtiene los datos de una tabla especifica de la DB PostgreSQL
    """
    try:
        cursor = conn.cursor()
        query = f"""
        SELECT id, resolution_number 
        FROM {table_name} 
        WHERE town = '25168'
        AND estado_actual_fecha_inicio BETWEEN '2024-11-01' AND '2024-12-12'
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
        return {row[0]: row[1] for row in result}  # Diccionario {id: valor}
    except Exception as e:
        log_message(f"Error consultando la tabla {table_name}: {e}")
        return {}
    
def parse_xml(file_path):
    """
    Lee y procesa el archivo XML.
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
    basándose en los valores de 'resolution_number' y 'record_value'.
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
        # CONSULTA DE DATOS EN LA DB DE POSTGRESQL
        db_table_name = 'data.tramite' # CAMBIAR NOMBRE DE LA TABLA, DE SER NECESARIO
        db_data = get_table_data(db_connection, db_table_name)

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
            FROM data.tramite
            WHERE town = '25168'
              AND estado_actual_fecha_inicio BETWEEN '2024-11-01' AND '2024-12-12'
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