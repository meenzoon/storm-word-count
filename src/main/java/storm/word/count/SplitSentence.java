package storm.word.count;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

// There are a variety of bolt types. In this case, use BaseBasicBolt
public class SplitSentence extends BaseBasicBolt {

  private static final long serialVersionUID = 1L;
  // private static final Logger logger = LogManager.getLogger(SplitSentence.class);

  // Execute is called to process tuples
  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputcollector) {
    // logger.info("SplitSentence execute: " + tuple.getSourceStreamId());
    // Get the sentence content from the tuple
    // String sentence = tuple.getString(0);
    String sentence = tuple.getStringByField("sentence");

    String[] word = sentence.split(" ");

    // for문 안에 word.length를 다른 형식으로 변경할 수 있게 생각
    for (int i = 0; i < word.length; i++) {
      // logger.info("word[" + i + "]:" + word[i]);
      word[i].replaceAll("\\s+", "");
      if (!word[i].equals("")) {
        basicOutputcollector.emit(new Values(word[i]));
      }
    }
    /*
     * 
     * // An iterator to get each word BreakIterator boundary = BreakIterator.getWordInstance();
     * 
     * // Give the iterator the sentence boundary.setText(sentence);
     * 
     * // Find the beginning first word int start = boundary.first();
     * 
     * // Iterate over each word and emit it to the output stream for (int end = boundary.next();
     * end != BreakIterator.DONE; start = end, end = boundary.next()) { // get the word String word
     * = sentence.substring(start, end);
     * 
     * // If a word is whitespace characters, replace it with empty word = word.replaceAll("\\s+",
     * "");
     * 
     * // if it's an actual word, emit it if (!word.equals("")) { collector.emit(new Values(word));
     * } }
     */
  }

  // Declare that emitted tuples contain a word field
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }
}
