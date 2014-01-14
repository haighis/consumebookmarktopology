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
