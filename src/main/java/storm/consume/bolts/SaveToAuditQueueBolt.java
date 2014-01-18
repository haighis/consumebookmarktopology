package storm.consume.bolts;

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
import storm.consume.model.BookmarkUrl;
import storm.consume.util.Conf;
import storm.consume.util.UrlUtil;
import storm.trident.state.Serializer;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SaveToAuditQueueBolt extends BaseRichBolt { //BaseBasicBolt

	OutputCollector _collector;
	
	private Jedis jedis;
	private String host;
    private int port;
     
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		host = stormConf.get(Conf.REDIS_HOST_KEY).toString();
        port = Integer.valueOf(stormConf.get(Conf.REDIS_PORT_KEY).toString());
        
        connectToRedis();
	}
	
	private void connectToRedis() {
        jedis = new Jedis(host, port);
        jedis.connect();
        //System.out.println("-IS CONNECTED TO REDIS----------------------------------------------------------" + jedis.isConnected());
    }
	
	public void cleanup() {}

	/**
	 * The bolt will receive the bookmark url from spout 
	 * and use Redis rpush to push the serialized object into a redis queue.
	 * 
	 *  The bookmark url object is then emitted to the audit bolt
	 */
	@Override
	public void execute(Tuple input) { // , BasicOutputCollector collector
		
		try {
		
			BookmarkUrl url = (BookmarkUrl)input.getValueByField("urlaudit");//FieldNames.LOG_ENTRY
			//LogEntry entry = (LogEntry)input.getValueByField(FieldNames.LOG_ENTRY);
			//String url = input.getString(0);
			
			System.out.println("------------------------------------------------------------------");
			System.out.println("-------------------Audit BOLT ------------------------------------");
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
	    	
			jedis.rpush(storm.consume.util.Conf.GetAuditQueueName(), storm.consume.util.Serializer.serialize(url));
			
	    	//byte[] bb = jedis.rpop(util.Conf.GetWordCountUrlsQueueName());
	    	
	    	//BookmarkUrl pUrl = (BookmarkUrl)util.Serializer.deserialize(bb);
			System.out.println("-IS CONNECTED TO REDIS----------------------------------------------------------" + jedis.isConnected());
	    	//System.out.println("after rpop bookmark - " + pUrl.getUrl());
	    	System.out.println("------------------------------------------------------------------");
	    	
			_collector.emit(new Values(url));
			_collector.ack(input);
			
			//collector.emit(new Values(entry.getUserId(), product, categ));
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
