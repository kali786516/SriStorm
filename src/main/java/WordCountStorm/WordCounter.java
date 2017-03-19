package WordCountStorm;

/**
 * Created by kalit_000 on 18/03/2017.
 */
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class WordCounter extends BaseBasicBolt{

    Map<String, Integer> counters;
    Integer id;
    String name;
    String fileName;
    public void prepare(Map stormConf, TopologyContext context) {
        /*A map to track words and their counts*/
        this.counters = new HashMap<String, Integer>();
        /*same as bash session Id $$ we are getting component ID and taskID*/
        /*imagine parallel Bolts running parallel and writing to same file which will be a conflict*/
        this.name = context.getThisComponentId(); // component name
        this.id = context.getThisTaskId();  //task id
        this.fileName = stormConf.get("dirToWrite").toString()+
                "output"+"-" +name+id +".txt";
    }


    public void execute(Tuple input,BasicOutputCollector collector) {
        /*assign tuple to input varialbe*/
        /*assign input to word varialbe*/
        String word = input.getString(0);

        String[] line=word.split(" ");

        for (String words:line){
            if(!counters.containsKey(word)) {
                counters.put(words, 1);
            }
            else{ /*if word value exists in the hashmap increment 1 to it*/
                Integer c = counters.get(words) + 1;
                counters.put(word, c);
        }
        }

        /*check whether word is in our hashmap*/
        /*if the word doesnt exists in the hashmap create a new key and assign a value*/

       // if(!counters.containsKey(word)) {
           // counters.put(word, 1);
        //}else{ /*if word value exists in the hashmap increment 1 to it*/
          //  Integer c = counters.get(word) + 1;
           // counters.put(word, c);
       // }


    }


    public void cleanup() {
           /*write the op to file*/
        try{
            PrintWriter writer = new PrintWriter(fileName, "UTF-8");

            for(Map.Entry<String, Integer> entry : counters.entrySet()){
                writer.println(entry.getKey()+": "+entry.getValue());
            }

            writer.close();
        }
        catch (Exception e){}


    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

}
