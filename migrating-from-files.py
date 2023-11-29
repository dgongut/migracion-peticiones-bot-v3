from config import *
import mysql.connector
import time
import re
import pickle
import json

def extract_name_from_string(cad):
    # Patrón de expresión regular para extraer el nombre
    pattern = r'<a.*?>(.*?)</a>'

    # Buscar coincidencias en la cadena
    match = re.search(pattern, cad)

    # Si se encuentra una coincidencia, devuelve el grupo capturado (nombre)
    if match:
        return match.group(1)
    else:
        # Si no se encuentra ninguna coincidencia, devuelve None o maneja el caso según tus necesidades
        return cad

def read_cache_item(file_path):
    with open(file_path, 'rb') as file:
        return pickle.load(file)

def is_filmaffinity_link(link):
    return "filmaffinity" in link

def url_to_film_code(url):
    numeroPelicula = None
    if is_filmaffinity_link(url):
        numeroPelicula = re.search(r'film(\d+)\.html', url)
    else:
        url = url.replace("\n", "")
        if not url.endswith("/"):
            url = f'{url}/'
        numeroPelicula = re.search(r'/tt(\d+)/', url)
    if numeroPelicula:
        numeroPelicula = numeroPelicula.group(1)
        return numeroPelicula
    else:
        raise ValueError(f'No se encontró un número de película en el enlace: {url}')

def create_tables_default(mydb):
    # Crea un cursor para ejecutar consultas
    cursor = mydb.cursor()

    # Verifica si las tablas ya existen
    cursor.execute("SHOW TABLES LIKE 'usuarios'")
    usuarios_exists = cursor.fetchone()
    print(f'TABLE usuarios existe: {usuarios_exists}')

    cursor.execute("SHOW TABLES LIKE 'peticiones'")
    peticiones_exists = cursor.fetchone()
    print(f'TABLE peticiones existe: {peticiones_exists}')

    cursor.execute("SHOW TABLES LIKE 'cache'")
    cache_exists = cursor.fetchone()
    print(f'TABLE cache existe: {cache_exists}')

    cursor.execute("SHOW TABLES LIKE 'status'")
    status_exist = cursor.fetchone()
    print(f'TABLE status existe: {status_exist}')

    cursor.execute("SHOW TABLES LIKE 'webpage'")
    webpage_exist = cursor.fetchone()
    print(f'TABLE webpage existe: {webpage_exist}')

    # Si las tablas no existen, créalas
    if not usuarios_exists:
        # Crear tabla de usuarios
        cursor.execute("""
            CREATE TABLE usuarios (
                chat_id BIGINT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                username VARCHAR(255),
                allowed BOOLEAN DEFAULT TRUE
            )
        """)

    if not status_exist:
        # Crear tabla de status
        cursor.execute("""
            CREATE TABLE status (
                id INT PRIMARY KEY,
                description VARCHAR(255) NOT NULL
            )
        """)

    if not webpage_exist:
        # Crear tabla de status
        cursor.execute("""
            CREATE TABLE webpage (
                id INT PRIMARY KEY,
                description VARCHAR(50) NOT NULL
            )
        """)

    if not cache_exists:
        # Crear tabla de cache
        cursor.execute("""
            CREATE TABLE cache (
                clave VARCHAR(255) PRIMARY KEY,
                valor TEXT
            )
        """)

    if not peticiones_exists:
        # Crear tabla de peticiones
        cursor.execute("""
            CREATE TABLE peticiones (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                film_code VARCHAR(255) NOT NULL,
                webpage_id INT NOT NULL,
                status_id INT NOT NULL,
                FOREIGN KEY (chat_id) REFERENCES usuarios(chat_id),
                FOREIGN KEY (status_id) REFERENCES status(id),
                FOREIGN KEY (webpage_id) REFERENCES webpage(id)
            )
        """)
    
    print("Tablas creadas")
    print("Insertando valores por defecto")
    cursor.execute('INSERT INTO status (id, description) VALUES (0, "pendiente")')
    cursor.execute('INSERT INTO status (id, description) VALUES (1, "completada")')
    cursor.execute('INSERT INTO status (id, description) VALUES (2, "denegada")')
    cursor.execute('INSERT INTO webpage (id, description) VALUES (0, "filmaffinity")')
    cursor.execute('INSERT INTO webpage (id, description) VALUES (1, "imdb")')
    mydb.commit()
    # Cerrar la conexión
    cursor.close()

def create_tables_and_migrate():
    HOST, PORT = DATABASE_HOST.split(":")
    PORT = int(PORT)

    max_retries = 3
    retry_delay = 5  # segundos

    print("Esperando 10 segundos para comenzar")
    time.sleep(10)

    for _ in range(max_retries):
        try:
            # Configura la conexión a tu base de datos
            mydb = mysql.connector.connect(
                host=HOST,
                port=PORT,
                user=DATABASE_USER,
                password=DATABASE_PASSWORD,
                database=DATABASE_NAME
            )

            create_tables_default(mydb)

            cursor = mydb.cursor()
            # Leer datos de usuarios y peticiones completadas desde el fichero TXT
            with open(FICHERO_PETICIONES_COMPLETADAS, "r") as file:
                print("---Migrando completadas")
                for line in file:
                    print(f'Migrando: {line}')
                    data = line.strip().split("|")  # Ajusta el separador según tus datos
                    cursor.execute("INSERT INTO usuarios (chat_id, name, username) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE name=%s, username=%s", (data[0], extract_name_from_string(data[1]), "", extract_name_from_string(data[1]), ""))
                    cursor.execute("INSERT INTO peticiones (chat_id, film_code, webpage_id, status_id) VALUES (%s, %s, %s, %s)", (data[0], url_to_film_code(data[2]), 0 if is_filmaffinity_link(data[2]) else 1, 1))

            # Leer datos de peticiones sin completar desde el fichero TXT
            with open(FICHERO_PETICIONES, "r") as file:
                print("---Migrando pendientes")
                for line in file:
                    print(f'Migrando: {line}')
                    data = line.strip().split("|")  # Ajusta el separador según tus datos
                    cursor.execute("INSERT INTO usuarios (chat_id, name, username) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE name=%s, username=%s", (data[0], extract_name_from_string(data[1]), "", extract_name_from_string(data[1]), ""))
                    cursor.execute("INSERT INTO peticiones (chat_id, film_code, webpage_id, status_id) VALUES (%s, %s, %s, %s)", (data[0], url_to_film_code(data[2]), 0 if is_filmaffinity_link(data[2]) else 1, 0))

            print("Migrando cache de busquedas")
            # Obtener la lista de ficheros en la carpeta "./cache"
            cache_folders = [SEARCH_FOLDER]

            # Iterar sobre las carpetas y migrar datos a la tabla de cache
            for cache_folder in cache_folders:
                # Obtener la lista de ficheros en la carpeta
                cache_files = [f for f in os.listdir(cache_folder) if os.path.isfile(os.path.join(cache_folder, f))]

                # Iterar sobre los ficheros y migrar datos a la tabla de cache
                for file_name in cache_files:
                    cache_key = file_name  # Usar el nombre del fichero como clave en la tabla
                    cache_value = read_cache_item(os.path.join(cache_folder, file_name))

                    # Insertar datos en la tabla de cache
                    cursor.execute("INSERT INTO cache (clave, valor) VALUES (%s, %s)", (cache_key, json.dumps(cache_value).strip()))

            # Confirmar los cambios en la base de datos
            mydb.commit()

            # Cerrar la conexión
            cursor.close()
            mydb.close()
            print("Migracion completa. SALIENDO.")
            # Si la conexión tuvo éxito, salir del bucle
            break
        except mysql.connector.Error as err:
            print(f"Error de conexión: {err}")
            print("Reintentando en {} segundos...".format(retry_delay))
            time.sleep(retry_delay)
    else:
        print(f"No se pudo conectar después de {max_retries} intentos.")
    return 0

# Llamar a la función para crear tablas y migrar datos si es necesario
create_tables_and_migrate()
