
package RESTful_Interface.MachineToolDataCollector;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;

@Path("/file")
public class UploadFileService
{
	private UnZip      unzip = new UnZip();
	private HdfsConnection conn;; // initial object
	
    @GET 
    @Path("/getIt")
    @Produces("text/plain")
    public String getIt(String fileName) throws IOException {
    	
        return "Successful Deployment of MachineToolDataStorageService";
    }
	
	@POST
	@Path("/upload")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadFile(@FormDataParam("file") InputStream uploadedInputStream,
							   @FormDataParam("file") FormDataContentDisposition fileDetail)
	{
		String fileLocation = "/MTDC/bigDataCollector/receivedFile/";
		String fileName = fileDetail.getFileName();
		String uploadFilePath = fileLocation + fileName;
		String output = "File uploaded to : " + uploadFilePath;
		try{
			writeToFile(uploadedInputStream, uploadFilePath); // �x�s�ѫȤ�ݵo�e�L�Ӫ��ɮ�
			this.unzip.setPath(uploadFilePath, fileLocation, "password"); // �]�w���Y�ɮרӷ��P�����Y�ɮצs���m
			this.unzip.doIt(); // �}�l�����Y
			
			String[] inputFilePath  = uploadFilePath.split("\\.");
			String[] outputFilePath = fileDetail.getFileName().split("\\.");
			
			this.conn = new HdfsConnection("node1", "14000", "admin");
			boolean isSuccessful = this.conn.upload(inputFilePath[0] + ".csv", outputFilePath[0]); // �W���ɮר� webhdfs
			System.out.println("The upload to webhdfs Result : " + isSuccessful);

			return Response.status(200).entity(output).build();			
		}catch(Exception ex) {
			return Response.status(500).entity(ex.toString()).build();
		}

	}

	// save uploaded file to new location
	private void writeToFile(InputStream uploadedInputStream, String uploadedFileLocation)
	{
		try
		{
			OutputStream out = new FileOutputStream(new File(uploadedFileLocation));
			
			int read = 0;
			byte[] bytes = new byte[1024];

			out = new FileOutputStream(new File(uploadedFileLocation));
			while((read = uploadedInputStream.read(bytes)) != -1)
			{
				out.write(bytes, 0, read);
			}
			
			out.flush();
			out.close();
		}
		
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
}