package YahooFinanceStorm;

import WordCountStorm.WordCounter;
import WordCountStorm.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by kalit_000 on 18/03/2017.
 */
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyYahoo {

    public static void main(String[] args) throws InterruptedException {

        //Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Yahoo-Finance-Spout", new yfSpout());
        builder.setBolt("Yahoo-Finance-Bolt", new yfBolt())/*in bolt we need to mention from which spout its recieving data from */
                .shuffleGrouping("Yahoo-Finance-Spout");/*here we are mentioning the source spout where data is coming from*/
        /*Grouping used to control data flow*/

        StormTopology topology = builder.createTopology();
        //Configuration
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("fileToWrite", "C:\\Users\\kalit_000\\Desktop\\pluralsight_hadoop\\storm_output\\output.txt");

        //Submit Topology to cluster
        LocalCluster cluster=new LocalCluster();
        try{
            cluster.submitTopology("Stock-Tracker-Topology", conf, topology);
            Thread.sleep(10000);}
        finally {
            cluster.shutdown();
        }
    }




}
