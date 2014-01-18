Consume Bookmark Topology

A Storm topology that will consume new fireflyy bookmarks and store them in 
redis for later processing by other storm topologies

ConsumeBookmarkSpout --> SaveToAuditQueueBolt --> SaveToLocalQueuesBolt (scrnur (Screenshot service), HTML source service, HTML to simple content view, total word count, tfidf, term summary grouping (grouping of words), google search api result suggestions, tag suggestions based on tfidf) 

Sample usage

You need to have java and maven setup and working prior to running this sample.

Compile the code first and then run the 
topology. From a windows commpand prompt execute the following.

Compile code first using the command:

mvn clean

mvn compile

Then execute the topology:

mvn exec:java -Dexec.mainClass="ConsumeBookmarkTopologyMain" 

If you are on windows you may get a build failure stating it cannot delete a
temporary log file. This is permission issue on windows which can be ignored. 
If you scroll up your command window you will see the topology ran successfully.

How to submit to a production storm cluster

/opt/storm-0.9.0.1/bin/storm jar /path/to/jar/consumebookmarktopology-0.0.1-SNAPSHOT.jar storm.consume.ConsumeBookmarkTopologyMain

https://github.com/nathanmarz/storm/wiki/Running-topologies-on-a-production-cluster


Create Java jar with dependencies

mvn -f pom.xml package

Submit Test Topology 

/opt/storm-0.9.0.1/bin/storm jar /home/haighis/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.starter.ExclamationTopology wills-test-topology

Submit Consume Bookmark Toplogy to Storm in production cluster

/opt/storm-0.9.0.1/bin/storm jar /home/[user]/[jarname].jar storm.consume.ConsumeBookmarkTopologyMain [SOMEHOSTNAME] 1

View Topology in Production Cluster

/opt/storm-0.9.0.1/bin/storm list

Kill Topology in Production Cluster

/opt/storm-0.9.0.1/bin/storm kill ConsumeBookmarkTopology