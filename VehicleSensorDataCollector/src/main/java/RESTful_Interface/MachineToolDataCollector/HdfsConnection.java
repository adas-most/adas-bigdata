package RESTful_Interface.MachineToolDataCollector;

import java.io.FileInputStream;
import java.io.IOException;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

public class HdfsConnection
{
	private static final String CONNECT_HOST = "127.0.0.1";
	private static final String PORT = "14000";
	private static final String USER = "admin";
	
    private String host = "127.0.0.1";
    private String port = "14000";
    private String user = "user of HDFS";
    private String url;
    private String home = "testFile";

    private HttpClient client = new HttpClient();
//    private PutMethod    put  = new PutMethod();
//    private DeleteMethod del  = new DeleteMethod();

    /***** using default parameter constructor *****/
    public HdfsConnection() { /***/ }

    /***** using custom parameter constructor *****/
    public HdfsConnection(String _host, String _port, String _user)
    {
        this.host = _host;
        this.port = _port;
        this.user = _user;
    }
    
    

    /***** build the request URL ******/
    private String buildUploadURL(String outputFilePath, int method)
    {
        String fileURL = "";
        String config = "";

        switch(method)
        {
            // upload file method
            case 1:
                fileURL = "http://" + this.host + ":" + this.port + "/webhdfs/v1/user/" + this.user + "/" + this.home + "/" + outputFilePath;
                config = "?user.name=" + this.user + "&op=CREATE&data=true";
                break;

            // delete method
            case 2:
                fileURL = "http://" + this.host + ":" + this.port + "/webhdfs/v1/user/" + this.user + "/" + this.home;
                config = "?user.name=" + this.user + "&op=DELETE&recursive=true";
                break;

            // list dir method
            case 3:
                fileURL = "http://" + this.host + ":" + this.port + "/webhdfs/v1/user/" + this.user+ "/" + this.home;
                config = "?user.name=" + this.user + "&op=LISTSTATUS";
                break;

            default:
                break;
        }

        return fileURL + config;
    }

    /***** upload file with PUT http request *****/
    public boolean upload(String inputFilePath, String outputFilePath)
    {
        this.url = buildUploadURL(outputFilePath, 1); // upload file method (number1) 

        HttpClient client = this.client;
        PutMethod  put    = new PutMethod(this.url);

        // 如果連線失敗，將會重新連線 (3次)
        put.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        try
        {
            put.setRequestHeader("content-type", "application/octet-stream");
            put.setRequestBody(new FileInputStream(inputFilePath));

            int statusCode = client.executeMethod(put);

            /***** 以下程式碼為偵測連線錯誤使用 *****/
            /**
            if(statusCode != HttpStatus.SC_OK)
            {
                System.err.print("Method failed: " + put.getStatusLine());
            }

            byte[] responseBody = put.getResponseBody();
            System.out.println(new String(responseBody));
            **/

            return true;
        }

        catch(HttpException httpexc)
        {
            System.err.println("Fatal protocol violation: " + httpexc.getMessage());
            httpexc.printStackTrace();
            return false;
        }

        catch(IOException ioexc)
        {
            System.err.println("Fatal transport error: " + ioexc.getMessage());
            ioexc.printStackTrace();
            return false;
        }

        finally
        {
            // 無論如何都一定要釋放連接
            put.releaseConnection();
        }
    }

    /***** remove all file in the dir *****/
    public boolean remove_dir()
    {
        this.url = buildUploadURL(this.home, 2); // delete method (number2) 

        HttpClient   client = this.client;
        DeleteMethod del    = new DeleteMethod(this.url);

        // 如果連線失敗，將會重新連線 (3次)
        del.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        try
        {
            int statusCode = client.executeMethod(del);

            /***** 以下程式碼為偵測連線錯誤使用 *****/
            /**
            if(statusCode != HttpStatus.SC_OK)
            {
                System.err.print("Method failed: " + del.getStatusLine());
            }

            byte[] responseBody = del.getResponseBody();
            System.out.println(new String(responseBody));
            **/

            return true;
        }

        catch(HttpException httpexc)
        {
            System.err.print("Fatal protocol violation: " + httpexc.getMessage());
            httpexc.printStackTrace();
            return false;
        }

        catch(IOException ioexc)
        {
            System.err.println("Fatal transport error: " + ioexc.getMessage());
            ioexc.printStackTrace();
            return false;
        }

        finally
        {
            // 無論如何都一定要釋放連接
            del.releaseConnection();
        }
    }

}