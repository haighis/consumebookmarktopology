import spouts.ConsumeBookmarkSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.SaveToAuditQueueBolt;
import bolts.SaveToLocalQueuesBolt;

public class ConsumeBookmarkTopologyMain {
	public static void main(String[] args) throws InterruptedException {
         
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("get-bookmark-url-azure", new ConsumeBookmarkSpout());
		builder.setBolt("save-to-audit", new SaveToAuditQueueBolt()).shuffleGrouping("get-bookmark-url-azure");
		builder.setBolt("save-to-redis", new SaveToLocalQueuesBolt()).shuffleGrouping("save-to-audit");
				
        //Configuration
		Config conf = new Config();
		conf.put(util.Conf.REDIS_HOST_KEY, "localhost");
		conf.put(util.Conf.REDIS_PORT_KEY, util.Conf.DEFAULT_JEDIS_PORT);
		
		conf.setDebug(true);
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCl asdfasdfuster cluster = new LocalCluster();
		cluster.submitTopology("ConsumeBookmarkTopology", conf, builder.createTopology());
		Thread.sleep(1000);
		//cluster.shutdown();
	}
}
