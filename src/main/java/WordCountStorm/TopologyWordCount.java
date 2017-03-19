package WordCountStorm;

/**
 * Created by kalit_000 on 18/03/2017.
 */
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyWordCount {

    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());//word count reader spout
        builder.setBolt("word-counter", new WordCounter(),2) // word count counter bolt
                .shuffleGrouping("word-reader");


        Config conf = new Config();
        conf.put("fileToRead", "C:\\Users\\kalit_000\\Desktop\\pluralsight_hadoop\\storm_output\\word_count_input.txt");
        conf.put("dirToWrite", "C:\\Users\\kalit_000\\Desktop\\pluralsight_hadoop\\storm_output\\");
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("WordCounter-Topology", conf, builder.createTopology());
            Thread.sleep(10000);
        }
        finally {
            cluster.shutdown();
        }


    }


}
