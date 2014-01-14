package bolts;

import model.BookmarkUrl;

import org.joda.time.DateTime;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import storm.trident.state.Serializer;
import util.Conf;
import util.UrlUtil;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SaveToLocalQueuesBolt extends BaseBasicBolt {

	private Jedis jedis;
	private String host;
    private int port;
    //OutputCollector collector;
    
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		host = stormConf.get(Conf.REDIS_HOST_KEY).toString();
        port = Integer.valueOf(stormConf.get(Conf.REDIS_PORT_KEY).toString());
        //, OutputCollector collector
        connectToRedis();
        System.out.println("------------------------------------------------------------------");
        System.out.println("------------------------------------------------------------------");
        System.out.println("after connect to redis");
        System.out.println("------------------------------------------------------------------");
        System.out.println("------------------------------------------------------------------");
	}
	
	private void connectToRedis() {
        jedis = new Jedis(host, port);
        jedis.connect();
        System.out.println("-IS CONNECTED TO REDIS----------------------------------------------------------" + jedis.isConnected());
    }
	
	public void cleanup() {}

	/**
	 * The bolt will receive the bookmark url from spout 
	 * and use Redis rpush to push the serialized object into a redis queue.
	 * 
	 *  The bookmark url object is then emitted to the audit bolt
	 */
	@Override
	public void execute(Tuple input , BasicOutputCollector collector) { //
		
		try {
		
			BookmarkUrl url = (BookmarkUrl)input.getValueByField("bookmark");//FieldNames.LOG_ENTRY
			//LogEntry entry = (LogEntry)input.getValueByField(FieldNames.LOG_ENTRY);
			//String url = input.getString(0);
			
			System.out.println("------------------------------------------------------------------");
			System.out.println("-------------------Save to local queue BOLT ------------------------------------------");
	    	System.out.println("------------------------------------------------------------------");
	    	System.out.println("------------------------------------------------------------------");
			System.out.println(" bookmark url: " + url.getUrl());
			System.out.println("------------------------------------------------------------------");
	    	System.out.println("------------------------------------------------------------------");
	    	System.out.println("------------------------------------------------------------------");
	    	
	    	if(!jedis.isConnected())
	    	{
	    		connectToRedis();
	    	}
	    	
	    	//jedis.set(url.getUrl(), url.getUrl());
	    	//jedis.rpush("urls", url.getUrl());
	    	//jedis.rpush(util.Conf.GetAuditQueueName(), util.Serializer.serialize(url));
			jedis.rpush(util.Conf.GetWordCountUrlsQueueName(), util.Serializer.serialize(url));
			jedis.rpush(util.Conf.GetScrnurQueueName(), util.Serializer.serialize(url));
			
	    	//byte[] bb = jedis.rpop(util.Conf.GetWordCountUrlsQueueName());
	    	
	    	//BookmarkUrl pUrl = (BookmarkUrl)util.Serializer.deserialize(bb);
			System.out.println("-IS CONNECTED TO REDIS----------------------------------------------------------" + jedis.isConnected());
	    	//String akey = jedis.get("johntest");
	    	//System.out.println("after rpop bookmark - " + pUrl.getUrl());
	    	
	    	System.out.println("---------------------------AKEYYY-----------------------");
	    	System.out.println("---------------------------AKEYYY-----------------------");
	    	//System.out.println("a key " + akey);
	    	System.out.println("------------------------------------------------------------------");
	    	
			collector.emit(new Values(url));
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("in bolt exception" + e.getMessage()); //e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("bookmark"));
	}
}
