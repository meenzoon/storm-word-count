package storm.word.count;

import java.util.Map;
import java.util.Random;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class RandomSentenceSpout extends BaseRichSpout {

  private static final long serialVersionUID = 1L;

  // Collector used to emit output
  SpoutOutputCollector collector;

  Random _rand;

  private final String[] sentences = new String[] {"the cow jumped over the moon",
      "an apple a day keeps the doctor away", "four score and seven years ago",
      "snow white and the seven dwarfs", "i am at two with nature"};

  // Open is called when an instance of the class is created
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
    // Set the instance collector to the one passed in
    this.collector = spoutOutputCollector;
    // For randomness
    _rand = new Random();
  }

  // Emit data to the stream
  @Override
  public void nextTuple() {
    // Sleep for a bit
    Utils.sleep(100);

    // sentences 데이터 중 한 개 선택
    String sentence = sentences[_rand.nextInt(sentences.length)];

    // Emit the sentence
    this.collector.emit(new Values(sentence));
  }

  // Ack is not implemented since this is a basic example
  @Override
  public void ack(Object id) {}

  // Fail is not implemented since this is a basic example
  @Override
  public void fail(Object id) {}

  // Declare the output fields. In this case, an sentence
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sentence"));
  }
}
