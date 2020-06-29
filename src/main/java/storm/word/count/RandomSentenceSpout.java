package storm.word.count;

import java.util.Map;
import java.util.Random;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class RandomSentenceSpout extends BaseRichSpout {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogManager.getLogger(RandomSentenceSpout.class);

  SpoutOutputCollector _collector;
  Random _rand;

  // Open is called when an instance of the class is created
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    logger.info("RandomSentenceSpout Open");

    _collector = collector;
    _rand = new Random();
  }

  // Emit data to the stream
  @Override
  public void nextTuple() {
    // logger.info("RandomSentenceSpout NextTuple");
    Utils.sleep(100);

    String[] sentences = new String[] {"the cow jumped over the moon",
        "an apple a day keeps the doctor away", "four score and seven years ago",
        "snow white and the seven dwarfs", "i am at two with nature"};

    String sentence = sentences[_rand.nextInt(sentences.length)];
    _collector.emit(new Values(sentence));

  }

  // msgId(?)를 이용하여 Spout에서 방출 된 튜플이 완전히 처리된것이 확인됐을때
  @Override
  public void ack(Object id) {
    logger.info("Spout Completed: " + id.toString());
  }

  // msgId(message id)를 이용하여 가져온 튜플이 처리에 실패했을때
  @Override
  public void fail(Object id) {
    logger.error("Spout Erorr: " + id.toString());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sentence"));
  }
}
