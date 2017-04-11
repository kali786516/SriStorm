package Trident;

/**
 * Created by kalit_000 on 4/8/17.
 */

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class splitFunction extends BaseFunction {

    public void execute(TridentTuple tuple, TridentCollector collector) {

        String sentence = tuple.getString(0);
        String[] words = sentence.split(" ");
        for(String word : words){
            word = word.trim();
            if(!word.isEmpty()){
                collector.emit(new Values(word));
            }
        }

    }

}
