package Trident;

/**
 * Created by kalit_000 on 4/8/17.
 */

import WordCountStorm.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.tuple.Fields;
import Trident.WordReaderSpotTrident;
import Trident.splitFunction;

public class helloWorldTrident {

    public static void main(String[] args) throws Exception {

       TridentTopology topology=new TridentTopology();

       topology.newStream("Lines",new WordReaderSpotTrident())
               .each(new Fields("word"),new splitFunction(),new Fields("word_split"))
               .each(new Fields("word_split"),new Debug());

       Config conf=new Config();
       conf.setDebug(true);
       conf.put("fileToRead","/Users/kalit_000/storm_op/sample.txt");


        //Topology run
        //Submit Topology to cluster
        LocalCluster cluster=new LocalCluster();
        try{
            cluster.submitTopology("Trident-Topology", conf, topology.build());
            Thread.sleep(10000);}
        finally {
            cluster.shutdown();
        }


    }

}
