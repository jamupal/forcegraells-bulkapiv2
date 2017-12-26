# README #

Este repositorio implementa un cliente Java simpl sobre la BULK API v2 de Salesforce, para probar sus servicios.

Se utiliza para medir su rendimiento y compararlo con v1, en la entrada del blog: https://forcegraells.com/2017/12/23/bulk-api-v2.

## Cómo usarlo ##

- Al servicio un proyecto Maven, solo es necesario la ejecución del POM, que genera un jar (ya disponible en el directorio target)

- Para ejecutarlo: ```java -jar bulkapiv2-0.0.1-SNAPSHOT.jar fichero_propiedades```

- El fichero de propiedades debe tener el siguiente formato y contenido:

```
#Datos para la conexion

USERNAME = username@dominio.com
PASSWORD = ******
TOKEN = ******
LOGIN_URL = https://login/test.salesforce.com
CLIENT_ID = ******
CLIENT_SECRET = ******
API_VERSION = v41.0

#Propiedades para la creación de Job
OBJETO_DESTINO = Persona__c
FORMATO_FICHERO_DATOS = CSV
OPERACION_BULK = insert
CARACTER_FINAL_LINEA = CRLF
SEPARADOR_COLUMNAS = SEMICOLON
MAX_POOLING = 10
MILLIS_POOLING = 3000

#Fichero Datos
PATH_FICHERO_DATOS = Personas1M.csv

```

- Los valores para las propiedades del Job, se encuentran en la documentación oficial de Salesforce

- La invocación del proyecto viene determinada por las propiedades que se especifican

- El fichero de propiedades debe modificarse para proporcionar los parámetros de conexión y situarlo en el directorio /src/main/resources.

Cualquier comentario es bienvenido: esteve.graells@gmail.com