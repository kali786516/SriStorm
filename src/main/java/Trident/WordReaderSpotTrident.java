package Trident;

/**
 * Created by kalit_000 on 4/8/17.
 */
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;



public class WordReaderSpotTrident extends BaseRichSpout{

    private SpoutOutputCollector collector;

    private FileReader fileReader;
    private BufferedReader reader ;

    /*flag value to check end of file*/
    private boolean completed = false;

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        this.collector = collector;

        try {
            this.fileReader = new FileReader(conf.get("fileToRead").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
        }
        this.reader =  new BufferedReader(fileReader);
    }


    public void nextTuple()
    {
        /*only when lines are in the file*/
        if (!completed) {

            try {

                String word = reader.readLine();

                if (word != null) {
                    word = word.trim();
                    word = word.toLowerCase();
                    collector.emit(new Values(word));
                } else {
                    completed = true;
                    fileReader.close();
                    ;
                }


            }
            catch(Exception e){
                throw new RuntimeException("Error reading tuple", e);
            }


        }

    }

    /*output fields*/
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {declarer.declare(new Fields("word"));}


}
