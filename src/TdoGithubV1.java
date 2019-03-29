import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.io.*;
import java.net.*;
import org.apache.commons.io.FileUtils;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.nio.file.Files; 
import java.nio.file.Path; 
import java.nio.file.Paths; 
import java.util.Objects; 

class retVal
{
    public int retCode;
    public String retMessage;
}

class TdoGithubV1
{
  
    private static String getDate()
    {
        String retVal = "";
        
        try {
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            Date date = new Date();
            retVal = dateFormat.format(date);
        }   
        catch (Exception ex)
        {

        }   
        return retVal;
    }
    
    private static retVal getConfig(String path)
    {
        retVal _retVal = new retVal();
        _retVal.retCode = 0;

        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            String dlmLine = br.readLine();
            dlmLine = dlmLine.replaceAll("\\s","");
            String[] dlmLineParts = dlmLine.split("\\=");
            if (dlmLineParts.length != 2) {
                _retVal.retCode = -1;
                _retVal.retMessage = "Invalid config type found.  Bye.";
                br.close();
                return _retVal;
            }
            _retVal.retCode = 0;
            _retVal.retMessage = dlmLineParts[1];
            br.close();
        }
        catch (Exception ex) {
            _retVal.retCode = -1;
            _retVal.retMessage = ex.getMessage();
        }

        return _retVal;
    }

    // To read the json data from URL/API
    private static String getHTML(String urlToRead) throws Exception 
    {
      StringBuilder result = new StringBuilder();
      URL url = new URL(urlToRead);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
      String line;
      while ((line = rd.readLine()) != null) 
      {
         result.append(line);
      }
      rd.close();
      return result.toString();
    }


    private static retVal processTDMTableList(String pathTDMLoader, String logFilePath) 
    {
        retVal _retVal = new retVal();
        _retVal.retCode = 0;
        String tableLine = "";

        try {

            // get the tdm loader config
            BufferedReader tdmLoaderReader = new BufferedReader(new FileReader(pathTDMLoader));
            String serverName = tdmLoaderReader.readLine();
            serverName = serverName.replaceAll("\\s","").split("\\=")[1];
            String portNumber = tdmLoaderReader.readLine();
            portNumber = portNumber.replaceAll("\\s","").split("\\=")[1];
            String projectName = tdmLoaderReader.readLine();
            projectName = projectName.replaceAll("\\s","").split("\\=")[1];
            String tableList = tdmLoaderReader.readLine();
            tableList = tableList.replaceAll("\\s","").split("\\=")[1];
            String destDir = tdmLoaderReader.readLine();
            destDir = destDir.replaceAll("\\s","").split("\\=")[1];
            tdmLoaderReader.close();
            
            // table list is the file name of the list of tables.  This can be tabken directly from the loader config. 
            // No need to pass it as a separate command line parameter
            File logFile = new File(logFilePath);
            logFile.getParentFile().mkdirs();
            System.out.println(logFile);
            FileWriter logFileHandle = new FileWriter(logFile);
            System.out.println("Log file creation");
            logFileHandle.write(getDate() + " : Batch started.");
            logFileHandle.write(System.getProperty( "line.separator" ));
            tableList = "C:\\temp\\tdo\\input\\" + tableList;
            BufferedReader br = new BufferedReader(new FileReader(tableList));
            
            String url = "";
            while ((tableLine = br.readLine()) != null) {
                // process this line
                if (!(tableLine.length() > 0))
                    continue;
                String[] lineParts = tableLine.split("\\-");

                // build the url now
                // http://ftwwdtdmd001:8080/core/1.0/API/project/WAYBILL/1.0/datablocks/TACTGWB/1.0/
                //String url1 = "http://ftwwdtdmd001:8080/core/1.0/API/project/" + p_Project + "/" + p_Ver + "/datablocks/" + p_Dblock + "/" + p_Ver + "/"; 
                String pDblocks = lineParts[0];   
                String apiVer = "1.0";
                String dVer = lineParts[1]; 
                JSONObject output;
                String strJson = "";
                try {
                url = "http://" + serverName + ":" + portNumber + "/core/1.0/API/project/" + projectName + "/" + apiVer + "/datablocks/" + pDblocks + "/" + dVer + "/";
                System.out.println(url);
                String jsonString = "{\"infile\" : " + getHTML(url) + "}";
                //System.out.println(jsonString);
                output = new JSONObject(jsonString);
                JSONArray docs = output.getJSONArray("infile");
                strJson = docs.toString();
                //System.out.println(strJson);
                } catch (JSONException e) 
                {
                e.printStackTrace();
                }
                String outputFilePath = destDir + "\\" + lineParts[0] + ".json"; 
                File file = new File(outputFilePath);
                file.getParentFile().mkdirs();
                try {
                    FileWriter tableFile = new FileWriter(file);
                    tableFile.write(strJson);
                    tableFile.close();
                    // success. update the log.                    
                    String logMessage = getDate() + " : " + tableLine + " processed successfully.";
                    logFileHandle.write(logMessage);
                    logFileHandle.write(System.getProperty( "line.separator" ));
                    }
                catch (IOException e) {
                    String logMessage = getDate() + " : " + tableLine + " processed unsuccessfully.";
                    logFileHandle.write(logMessage);
                    logFileHandle.write(System.getProperty( "line.separator" ));
                    System.out.println(logMessage);
                }
            }
            
            logFileHandle.write(getDate() + " : Batch completed.");
            logFileHandle.close();
            br.close();

            //File[] files = new File(destDir).listFiles();
            //for (File file : files) {
                //if (file.isDirectory()) {
                    //System.out.println("Directory: " + file.getName());
                    //showFiles(file.listFiles()); // Calls same method again.
                //} else {
                    //System.out.println("File: " + file.getName());
                //    Files.write(directory.resolve(file.getName()), new byte[0]); 
                    

                //}
            //}
        }
        catch (Exception ex) {
            _retVal.retCode = -1;
            _retVal.retMessage = ex.getMessage();
            System.out.println(_retVal.retMessage);
        }

        return _retVal;
    }

    private static Path directory = null;

    private static void cloneGhub() throws IOException, InterruptedException
    { 
        System.out.println("Cloning started ...");
        String originUrl = "https://github.com/rvsapsu/tdo.git"; 
        directory = Paths.get("C:\\temp\\tdo"); 
        gitClone(directory, originUrl);         
    } 
    // To copy from source path tp github path
    //public static void Kopy(Path direcotry, String SrcInput, String GitIn) throws IOException, InterruptedException { 
    //    runCommand(directory, "copy", ""); 
    //} 

    public static void gitClone(Path directory, String originUrl) throws IOException, InterruptedException 
    { 
        System.out.println("GitClone started ...");
       // runCommand(directory.getParent(), "git", "clone", originUrl, directory.getFileName().toString()); 
        runCommand(directory.getParent(), "git", "clone", originUrl);
    }

    public static void gitStage(Path directory) throws IOException, InterruptedException 
    { 
        runCommand(directory, "git", "add", "-A"); 
    } 

    public static void gitCommit(Path directory, String message) throws IOException, InterruptedException
    { 
        runCommand(directory, "git", "commit", "-m", message); 
    } 

    public static void gitPush(Path directory) throws IOException, InterruptedException 
    { 
        runCommand(directory, "git", "push"); 
    } 
   
    public static void runCommand(Path directory, String... command) throws IOException, InterruptedException 
    { 
        
        Objects.requireNonNull(directory, "directory"); 
        if (!Files.exists(directory)) 
        { 
        throw new RuntimeException("can't run command in non-existing directory '" + directory + "'"); 
        } 

        ProcessBuilder pb = new ProcessBuilder() 
        .command(command) 
             .directory(directory.toFile()); 
        Process p = pb.start(); 
        StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream(), "ERROR"); 
        StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream(), "OUTPUT"); 
        outputGobbler.start(); 
        errorGobbler.start(); 
        int exit = p.waitFor(); 
        errorGobbler.join(); 
        outputGobbler.join(); 
        if (exit != 0) 
        {   
            throw new AssertionError(String.format("runCommand returned %d", exit));
        }     
    }
 
    public static void main(String[] args)
    {
        try
        {
            
            // check the parameters first
            //if (args.length != 3) 
            //{
            //    System.out.println("Usage: java util1 <data config file path> <tdmloader file path> <log file path>");
            //   return;
            //}
            System.out.println(" Main method started ..");
            cloneGhub();
            String fileArg1 = directory.toString() + "\\input\\dataconfig.cfg";
            System.out.println(" fileArg1 : " + fileArg1 );
            retVal configTypeRetVal = getConfig(fileArg1);
            if (configTypeRetVal.retCode == -1) 
            {
                System.out.println("configTypeRetVal not valid :" + configTypeRetVal.retCode );
                return;
            }
            String configType = configTypeRetVal.retMessage;
            if (configType.toUpperCase().equals("SQL")) 
            {
                System.out.println("Data Source listed as SQL. TDM Configuration not found. Exiting");
                return;
            }
            
            System.out.println("config type is processed.");
            // data source is NOT SQL
            String fileArg2 = directory.toString() + "\\input\\tdmloader.cfg";
            String fileArg3 = directory.toString() + "\\output\\TdoGithubTrace.log";
            retVal tableListRetVal = processTDMTableList(fileArg2, fileArg3);
            if (tableListRetVal.retCode == -1) 
            {
                System.out.println("tableListRetVal not valid :" + tableListRetVal.retCode );
                return;
            }
            
            // for source and target paths 
            //String SrcPath = "D:\\TDO\\Input\\*";
            //Kopy(directory,SrcPath,GithubPath);
            gitStage(directory); 
    		gitCommit(directory, "tdo test"); 
    		gitPush(directory); 
            
        }   
        catch (Exception ex) 
        {
            System.out.println(ex);
        }     
    }
    private static class StreamGobbler extends Thread 
    { 
 
        private final InputStream is; 
        private final String type; 

        private StreamGobbler(InputStream is, String type) { 
        this.is = is; 
        this.type = type; 
        } 

        @Override 
        public void run() { 
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is));) { 
            String line; 
            while ((line = br.readLine()) != null) { 
                System.out.println(type + "> " + line); 
            } 
        } catch (IOException ioe) { 
            ioe.printStackTrace(); 
        } 
        } 
    } 
}

