package utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ManageFile {

	private ManageFile() {
		
	}
	
	/**
	 * Crea archivo con los resultados fallidos
	 * 
	 * @param respuesta (Devuelve los resultados fallidos)
	 * @param ruta (ruta donde va a quedar guardado el archivo
	 * @param jobId(idjob, para nombrar el archivo
	 * 
	 */
	
	 public static void writeFile(String respuesta, String ruta, String jobId) {
    	 try {
    		 
    		 DateTimeFormatter time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
             String complement = "{" +time.format(LocalDateTime.now())+ "}" + " jobid:"+ jobId + " response: "; 
             respuesta = complement.concat(respuesta);
             File file = new File(ruta);
             if (!file.exists()) {
                 file.createNewFile();
             }
             
             FileWriter fw = new FileWriter(file);
             BufferedWriter bw = new BufferedWriter(fw);
             bw.write(respuesta);
             bw.close();
         } catch (Exception e) {
             e.printStackTrace();
         }
    }
	 
	 /**
	     * Mover el archivo de la carpeta de entrada a la carpeta de salida cuando se ha procesado
	     * @param folder_int(directorio de entrada)
	     * @param folder_out(directorio de salida)
	     */
	    
	    public static void changeFolder(String folder_int, String folder_out) {
	    	File fold_int = new File(folder_int);
	    	String name = fold_int.getName().replace(".csv", "");
	    	DateTimeFormatter time = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss");
	    	name = name.concat(time.format(LocalDateTime.now())).concat(".csv");
	    	folder_out = folder_out.concat(name);
	    	
			Path inputFolder = FileSystems.getDefault().getPath(folder_int);
	        Path ouputFolder = FileSystems.getDefault().getPath(folder_out);
	        
	        try {
	            Files.move(inputFolder, ouputFolder, StandardCopyOption.REPLACE_EXISTING);
	        } catch (IOException e) {
	            System.err.println(e);
	        }
		}

}
