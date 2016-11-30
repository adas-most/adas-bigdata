package RESTful_Interface.MachineToolDataCollector;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

public class UnZip
{
    private String source      = "C://zipFile.zip";
    private String destination = "D://";
    private String password    = "password";
	
    /***** using default parameter constructor *****/
    public UnZip() { /***/ }
	
    /***** using custom parameter constructor *****/
    public void setPath(String _source, String _destination, String _password)
    {
        this.source      = _source;
        this.destination = _destination;
        this.password    = _password;
    }
    
	// unzip source file to destination
	public void doIt()
	{
		try
		{
			ZipFile zipFile = new ZipFile(this.source);
			if(zipFile.isEncrypted()){ zipFile.setPassword(this.password); }
	        zipFile.extractAll(this.destination);
	    }
		
		catch (ZipException e)
		{
	        e.printStackTrace();
	    }
	}

}