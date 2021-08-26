/**
 * Cliente para pruebas de la BULK API v2 de Salesforce.
 * <p>
 * <P>Muy importante conocer y tener acceso a la documentación
 *
 * @link https://developer.salesforce.com/docs/atlas.en-us.api_bulk_v2.meta/api_bulk_v2/introduction_bulk_api_2.htm
 * <p>
 * Usado y comentado en: https://forcegraells.com/bulkapiv2
 * *
 * @author Esteve Graells
 * @version 1.0
 */


package graells.bulkapiv2;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import utils.ManageFile;


import static java.nio.charset.StandardCharsets.UTF_8;

public class Bulkapiv2Application {

    private static final Logger log = LoggerFactory.getLogger(Bulkapiv2Application.class);

    //Miembros de la clase, para albergar las propiedades, el token de conexión, la url real y sobretodo
    //el identificador de Job que se obtendrá al crearlo y que es necesario en todas las peticiones
    private static Properties prop = new Properties();
    private static String token = null;
    private static String urlInstance = null;
    private static String jobId = null;

    /**
     *
     * @param args Path al fichero de propiedades
     * */
    public static void main(String args[]) throws Exception {

        //Lecturas de las propiedasdes del job que se generar

        leerPropiedades(args);

        loginORG();

        if (crearJobSimple()) {

            enviarDatos();

            abortarCerrarJob( "CERRAR");
            
            log.info("Estado final del Job: " + poolingJobInfoHastaEstadoFinal());
            failedResults();
            log.info("Resultado finales del job: " + obtenerInfoJob().toString());
            //Podría ser más detallado, este log, verdad? TODO par el lector
        }
    }

    /**
     * Lectura de las propiedades
     *
     * @throws IOException Fichero no encontrado
     *
     * */
    private static void leerPropiedades(String[] args) throws IOException {

        String path = "";

        if (args.length > 0 ) {
            path = (args[0] == null || args[0].isEmpty()) ? "." : args[0];
        }else{
            System.out.println("Error: debe informarse como parámetro el path al fichero de propiedades");
            System.exit(0);
        }

        InputStream input = new FileInputStream(path);
        prop.load(input);

    }

    /**
     * Realiza una conexión con la ORG indicada en los miembros de la clase
     * Establece los valores de token y urlInstance si se consigue un login correcto
     *
     * @throws Exception No se establece conexión
     */
    private static void loginORG() throws Exception {

        ConexionSF con = new ConexionSF(
                prop.getProperty("USERNAME"),
                prop.getProperty("PASSWORD"),
                prop.getProperty("TOKEN"),
                prop.getProperty("LOGIN_URL"),
                prop.getProperty("CLIENT_ID"),
                prop.getProperty("CLIENT_SECRET")
        );

        if (con != null) {
            token = con.getloginAccessToken();
            urlInstance = con.getloginInstanceUrl();
        } else {
            throw new Exception("No se pudo establecer conexión con la ORG");
        }
    }

    /**
     * Creación de un Job mediante opciones por defecto de la API
     * Se incluye el Payload que sea necesario, cuyos valores estan definidos en fichero propiedades
     *
     * @return Devuelve el identificador el Job creado o null
     * @throws Exception Lectura de fichero errónea
     */
//    private static String crearJobMultipart() throws Exception {
//    	try {
//        HttpPost peticionPost = new HttpPost(urlInstance + "/services/data/" + prop.getProperty("API_VERSION") + "/jobs/ingest");
//
//        //Cabeceras: siguiendo lo indicado en la documentación
//        peticionPost.setHeader("Content-Type", "multipart/form-data; boundary=BOUNDARY");
//        peticionPost.setHeader("Accept", "application/json");
//        peticionPost.setHeader("Authorization", "Bearer " + token);
//
//        //Body: siguiendo especificación del documento. Formado por varios BOUNDARIES
//        String payload =
//                "--BOUNDARY\n" +
//                        "Content-Type: application/json\n" +
//                        "Content-Disposition: form-data; name=\"job\"\n\n" +
//
//                        "{\"object\":" + "\"" + prop.getProperty("OBJETO_DESTINO") + "\"" + "," +
//                        "\"contentType\":" + "\"" + prop.getProperty("FORMATO_FICHERO_DATOS") + "\"" + "," +
//                        "\"operation\":" + "\"" + prop.getProperty("OPERACION_BULK") + "\"" + "," +
//                        "\"externalIdFieldName\":" + "\"" + prop.getProperty("EXTERNAL_ID") + "\"" + "," +
//                        "\"lineEnding\":" + "\"" + prop.getProperty("CARACTER_FINAL_LINEA") + "\"" + "," +
//                        "\"columnDelimiter\":" + "\"" + prop.getProperty("SEPARADOR_COLUMNAS") + "\"" + "," +
//                        "\"jobType\":" + "\"Classic\"" +
//                        "}\n" +
//                        "\n--BOUNDARY\n" +
//
//                        "Content-Type: text/csv\n" +
//                        "Content-Disposition: form-data; name=\"content\"; filename=\"content\"\n\n";
//
//        payload += leerFicheroDatos(prop.getProperty("PATH_FICHERO_DATOS_ENTRADA"), UTF_8);
//        payload += "\n--BOUNDARY--";
//
//        StringEntity requestEntity = new StringEntity(payload, ContentType.APPLICATION_JSON);
//        peticionPost.setEntity(requestEntity);
//
//        String jobId = enviarPeticionPost(peticionPost);
//
//        log.info("El id del job creado es:" + jobId);
//
//        
//    	}catch (Exception e) {
//    		e.printStackTrace();
//		}
//    	return jobId;
//    }

    /**
     * Post a URL /services/data/vXX.X/jobs/ingest para crear un nuevo job, con las cabeceras que indica el documento
     * Hay que ser muy cuidadoso con los saltos de linea en el payload
     *
     * @return El identificador del job creado ó null
     * @throws Exception En caso de ejecución anómala del servicio
     *
     */
    private static Boolean crearJobSimple() throws Exception {

        HttpPost peticionPost = new HttpPost(urlInstance + "/services/data/" + prop.getProperty("API_VERSION") + "/jobs/ingest");

        //Cabeceras según indica el documento
        peticionPost.setHeader("Content-Type", "application/json;");
        peticionPost.setHeader("Accept", "application/json");
        peticionPost.setHeader("Authorization", "Bearer " + token);
        
        String externalIdFieldName = prop.getProperty("EXTERNAL_ID");
        if(!externalIdFieldName.equals("")){
        	externalIdFieldName = "\"externalIdFieldName\":" + "\"" + prop.getProperty("EXTERNAL_ID") + "\"" + ",";
        }
        //Body: los saltos de lineas son fundamentales.
        String payload =
                "{\"object\":" + "\"" + prop.getProperty("OBJETO_DESTINO") + "\"" + "," +
                        "\"contentType\":" + "\"" + prop.getProperty("FORMATO_FICHERO_DATOS") + "\"" + "," +
                        "\"operation\":" + "\"" + prop.getProperty("OPERACION_BULK") + "\"" + "," + externalIdFieldName +
                        "\"lineEnding\":" + "\"" + prop.getProperty("CARACTER_FINAL_LINEA") + "\"" + "," +
                        "\"columnDelimiter\":" + "\"" + prop.getProperty("SEPARADOR_COLUMNAS") + "\"" +
                "}\n";

        StringEntity requestEntity = new StringEntity(payload, ContentType.APPLICATION_JSON);
        peticionPost.setEntity(requestEntity);

        jobId = enviarPeticionPost(peticionPost);

        return (!jobId.isEmpty());
    }

    /**
     * Envia una petición Post preconfigurada
     *
     * @param post Contenido de la petición que se ejecutará mediante POST
     * @return el Identificador del job creado ó null
     *
     */
    private static String enviarPeticionPost(HttpPost post) {

        String jobId = null;

        try {
            HttpClient client = HttpClientBuilder.create().build();
            HttpResponse httpResponse = client.execute(post);

            String respuesta = IOUtils.toString(httpResponse.getEntity().getContent());
            JSONObject jsonObject = new JSONObject(respuesta);

            jobId = jsonObject.getString("id");

        } catch (Exception e) {
            e.printStackTrace();
        }

        return jobId;
    }

    /*
     * Get a la url /services/data/vXX.X/jobs/ingest/jobID para obtener la info de un Job
     *
     * @return la respuesta recibida de Salesforce en un objeto Json ó null
     *
     */
    private static JSONObject obtenerInfoJob() throws Exception {

        if (jobId== null) {
            throw new Exception("Id del Job es null");
        }

        //Petición GET sin parámetros ni Body
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet req = new HttpGet(urlInstance + "/services/data/" + prop.getProperty("API_VERSION") + "/jobs/ingest/" + jobId);

        //Cabeceras
        req.setHeader("Accept", "application/json");
        req.setHeader("Content-Type", "application/json; charset=UTF-8");
        req.setHeader("Authorization", "Bearer " + token);

        HttpResponse response = client.execute(req);
        String respuesta = IOUtils.toString(response.getEntity().getContent());

        JSONObject respuestaJson = null;

        if (!respuesta.contains("error") && !respuesta.contains("Error")) {
            respuestaJson = new JSONObject(respuesta);
        }
        
        return respuestaJson;
    }

    /**
     * Realiza pooling de la info de un Job, cada n segundos, hasta q el Job finaliza en estado "JobComplete" ó "Failed"
     *
     * @return El estado final del job
     * @throws Exception Identificador del Job es null
     *
     */
    private static String poolingJobInfoHastaEstadoFinal() throws Exception {

        if (jobId == null) {
            throw new Exception("Job id es null");
        }

        Integer MAX_POOLING = Integer.parseInt(prop.getProperty("MAX_POOLING"));
        Integer MILLIS_POOLING = Integer.parseInt(prop.getProperty("MILLIS_POOLING"));

        String estadoJob = obtenerInfoJob().get("state").toString();
        List<String> estadosFinales = Arrays.asList("JobComplete", "Failed");

        Integer poolCounting = 0;
        while ( (!estadosFinales.contains(estadoJob)) && (poolCounting < MAX_POOLING) )  {
            poolCounting++;

            log.info("El estado del job es: " + estadoJob);
            try {
                Thread.sleep(MILLIS_POOLING);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
            estadoJob = obtenerInfoJob().get("state").toString();
        }
        
        String folder_int= prop.getProperty("PATH_FICHERO_DATOS_ENTRADA");
        String folder_out = prop.getProperty("PATH_FICHERO_DATOS_SALIDA");
        if(estadoJob.equals("JobComplete")) {
        	ManageFile.changeFolder(folder_int, folder_out);
        }

        if ( poolCounting == MAX_POOLING ) return "Timeout de espera";

        return estadoJob;
    }

    /**
     * Llamada PATCH sobre la url /services/data/vXX.X/jobs/ingest/jobID para indicar a Salesforce que ejecute un cambio
     * de estado sobre el job, para cerrarlo y que se proceda a su ejecución o para cancelarlo
     *
     * @param operacion ABORATAR ó CERRAR
     * @throws Exception Si no se reciben id u operacion
     *
     */
    private static void abortarCerrarJob(String operacion) throws Exception {

        if ((jobId == null) || operacion.isEmpty()) {
            throw new Exception("Id del Job es null");
        }

        //Petición GET sin parámetros ni Body
        HttpClient client = HttpClientBuilder.create().build();
        HttpPatch req = new HttpPatch(urlInstance + "/services/data/" + prop.getProperty("API_VERSION") + "/jobs/ingest/" + jobId);
        System.out.println(req);
        //Cabeceras
        req.setHeader("Accept", "application/json");
        req.setHeader("Content-Type", "application/json; charset=UTF-8");
        req.setHeader("Authorization", "Bearer " + token);

        //Body
        String payload = null;

        if (operacion.equals("ABORTAR")) {
            payload = "{" + "\"state\": \"Aborted\"" + "}";

        } else if (operacion.equals("CERRAR")) {
            payload = "{" + "\"state\": \"UploadComplete\"" + "}";
        }

        StringEntity requestEntity = new StringEntity(payload, ContentType.APPLICATION_JSON);
        req.setEntity(requestEntity);

        //Envio de la petición
        HttpResponse response = client.execute(req);

        String respuesta = IOUtils.toString(response.getEntity().getContent());
        log.info("Resultado de cierre/abortar q: " + respuesta);
    }
    
    /**
     * Llamada PATCH sobre la url /services/data/vXX.X/jobs/ingest/jobID/failedResults para consultar los resultados fallidos
     * para luego guardar el resultado en un archivo
     *
     * 
     * @throws Exception Si no se recibe id
     *
     */
    private static void failedResults() throws Exception {

        
    	if (jobId== null) {
            throw new Exception("Id del Job es null");
        }

        //Petición GET sin parámetros ni Body
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet req = new HttpGet(urlInstance + "/services/data/" + prop.getProperty("API_VERSION") + "/jobs/ingest/"+jobId+"/failedResults/");

        //Cabeceras
        req.setHeader("Accept", "application/json");
        req.setHeader("Content-Type", "application/json; charset=UTF-8");
        req.setHeader("Authorization", "Bearer " + token);

        HttpResponse response = client.execute(req);
        
        String respuesta = IOUtils.toString(response.getEntity().getContent());
        String ruta = prop.getProperty("PATH_FAILED_RESULT") + jobId+".log";
        ManageFile.writeFile(respuesta, ruta);
    }
    
    

    /**
     * Llamada DELETE sobre la url /services/data/vXX.X/jobs/ingest/jobID para eliminar el job
     *
     * @return Estado final del Job
     * @throws Exception Identificador de Job es null
     *
     */
    private static String eliminarJob() throws Exception {

        if (jobId == null) {
            throw new Exception("Id del Job es null");
        }

        //Petición GET sin parámetros ni Body
        HttpClient client = HttpClientBuilder.create().build();
        HttpDelete req = new HttpDelete(urlInstance + "/services/data/" + prop.getProperty("API_VERSION") + "/jobs/ingest/" + jobId);

        //Cabeceras
        req.setHeader("Accept", "application/json");
        req.setHeader("Content-Type", "application/json; charset=UTF-8");
        req.setHeader("Authorization", "Bearer " + token);

        HttpResponse response = client.execute(req);
        //No se espera respuesta. Returns a status code of 204 (No Content), which indicates that the job was successfully deleted.

        log.info("Resultado de cierre/abortar q: " + response.getStatusLine().toString());

        return response.getStatusLine().toString();
    }

    /**
     * Llamada GET sobre la url /services/data/vXX.X/jobs/ingest para obtener la lista de jobs en curso
     * La petición admite filtros, que no he implementado, sin filtros los muestra todos
     *
     * @return Respuesta del servicio ó null. Debe iterarse sobre el array obteniendo job.get("id") y job.get("state"))
     * @throws Exception En caso de ejecución anómala del servicio
     *
     */
    private static JSONArray obtenerInfoTodosJobs() throws Exception {

        //Petición GET que no requiere Param ni Body
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet req = new HttpGet(urlInstance + "/services/data/" + prop.getProperty("API_VERSION") + "/jobs/ingest");

        //Cabeceras
        req.setHeader("Accept", "application/json");
        req.setHeader("Content-Type", "application/json; charset=UTF-8");
        req.setHeader("Authorization", "Bearer " + token);

        HttpResponse response = client.execute(req);
        String respuesta = IOUtils.toString(response.getEntity().getContent());
        //System.out.println("-ege- Jobs en la plataforma : " + respuesta);

        JSONArray jobsInfo = null;

        if (!respuesta.isEmpty()) {
            jobsInfo = new JSONObject(respuesta).getJSONArray("records");
        }

        log.info("La info de los Jobs es: " + jobsInfo);

        return jobsInfo;
    }

    /**
     * Durante el proceso de creación de Job simple, No multipart, la operación de carga de los datos del CSV
     * se realiza a posteriori de su creación.
     * Por tanto, esta función realiz un PUT sobre la url que se obtiene del Job, para cargar en el payload
     * el contenido del CSV y enviarlo.
     * Recalcar la importancia del contenido de las cabeceras siguiendo el documento oficial.
     *
     * @return Código de respuesta de la petición
     *
     * @throws Exception Identificador de Job es null
     *
     */
    private static String enviarDatos() throws Exception {

        JSONObject jobInfo = null;
        if (jobId == null) {
            throw new Exception("Job id es null");
        } else {
            //URL is provided in the contentUrl field in the response from Create a Job, or the response from a Job Info request on an open job.
            jobInfo = obtenerInfoJob();
        }

        String urluploadDatos = null;
        if (jobInfo != null) {
            urluploadDatos = jobInfo.get("contentUrl").toString();
        }

        HttpClient client = HttpClientBuilder.create().build();
        HttpPut req = new HttpPut(urlInstance + "/" + urluploadDatos);

        //Cabeceras
        req.setHeader("Content-Type", " text/csv"); //Atención esta header debe indicar el Content-Type adecuado
        req.setHeader("Accept", "application/json");
        req.setHeader("Authorization", "Bearer " + token);

        String payload = leerFicheroDatos(prop.getProperty("PATH_FICHERO_DATOS_ENTRADA"), UTF_8);

        StringEntity requestEntity = new StringEntity(payload, ContentType.TEXT_PLAIN);
        req.setEntity(requestEntity);

        HttpResponse response = client.execute(req);

        log.info("Resultado del envio de datos: " + response.getStatusLine().toString());
        
        return response.getStatusLine().toString();
    }

    /**
     * Lee el contenido de un fichero
     *
     * @param path Path relativo al fichero
     * @param encoding Charset de lectura del fichero
     *
     * @return Contenido del fichero
     * @throws IOException En caso de error de lectura del fichero
     *
     */
    private static String leerFicheroDatos(String path, Charset encoding)
            throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }
    
}