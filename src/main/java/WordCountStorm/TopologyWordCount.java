package WordCountStorm;

/**
 * Created by kalit_000 on 18/03/2017.
 */
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import WordCountStorm.alphaGrouping;

public class TopologyWordCount {

    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());//word count reader spout
        builder.setBolt("word-counter", new WordCounter(),2) // word count counter bolt
                //.shuffleGrouping("word-reader");
        //.fieldsGrouping("word-reader",new Fields("word"));// fields grouping based on word
        .customGrouping("word-reader",new alphaGrouping());


        Config conf = new Config();
        conf.put("fileToRead", "/Users/kalit_000/storm_op/word_count_input.txt");
        conf.put("dirToWrite", "/Users/kalit_000/storm_op/word_count_op/");
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
