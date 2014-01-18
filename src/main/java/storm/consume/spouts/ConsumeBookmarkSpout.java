package storm.consume.spouts;

import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import redis.clients.jedis.Jedis;
import storm.consume.model.BookmarkUrl;
import storm.consume.util.Conf;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ConsumeBookmarkSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("urlaudit"));
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }
    
    public void close() {
    }

    @Override
    public void nextTuple() {
      	try {
			
			Client client = ClientBuilder.newClient();
	    	Response res = client.target(storm.consume.util.Conf.AZURE_QUEUE_POPBOOKMARK).request("application/json").get();
	    	
	    	String bookmarkString = res.readEntity(String.class);
	    	
	    	if(!bookmarkString.contains("No new bookmarks"))
	    	{
	    		JSONParser jsonParser = new JSONParser();
	    		JSONObject jsonObject;
			
				jsonObject = (JSONObject) jsonParser.parse(bookmarkString);
				String bookmarkId = (String) jsonObject.get("bookmarkId");
				String url = (String) jsonObject.get("url");
				String userId = (String) jsonObject.get("userId");
				
				BookmarkUrl b = new BookmarkUrl(bookmarkId,url,userId);
		    
				System.out.println("------------------------------------------------------------------");
		    	System.out.println("-------------------SPOUT------------------------------------------");
		    	System.out.println("------------------------------------------------------------------");
		    	System.out.println("------------------------------------------------------------------");
		    	//System.out.println("bookmark from azure queue " + res.readEntity(String.class));
		    	System.out.println("bookmark from azure queue " + b.getUrl() + b.getUserId() + b.getBookmarkId());
		    	
		    	System.out.println("------------------------------------------------------------------");
		    	System.out.println("------------------------------------------------------------------");
		    	System.out.println("------------------------------------------------------------------");
		    	
				collector.emit(new Values(b));
	    	}
	    	else
	    	{
	    		System.out.println("-------------------SPOUT - no new bookmarks------------------------------------------");
	    		try { Thread.sleep(1000); } catch (InterruptedException e) {}
			}
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			System.out.println("in spout error: " + e.getMessage()); //e.printStackTrace();
		}
    }
    
    public void ack(Object msgId) {
    	
    }
    	
    public void fail(Object msgId) {
    }
}