package storm.consume.util;

import java.io.IOException;

import storm.consume.model.BookmarkUrl;

public class Conf {
    public static final String REDIS_HOST_KEY = "redisHost";
    public static final String REDIS_PORT_KEY = "redisPort";
    public static final String DEFAULT_JEDIS_PORT = "6379";
    public static final String AZURE_QUEUE_POPBOOKMARK = "host";
    
    public static byte[] GetWordCountUrlsQueueName()
    {
    	byte[] ar = null;
    	
    	try {
			ar = storm.consume.util.Serializer.serialize("counturls");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    	
    	return ar;
    }
   
    public static byte[] GetAuditQueueName()
    {
    	byte[] ar = null;
    	
    	try {
			ar = storm.consume.util.Serializer.serialize("newbaudit");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    	
    	return ar;
    }
    
    public static byte[] GetScrnurQueueName()
    {
    	byte[] ar = null;
    	
    	try {
			ar = storm.consume.util.Serializer.serialize("newscrnur");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    	
    	return ar;
    }
}
