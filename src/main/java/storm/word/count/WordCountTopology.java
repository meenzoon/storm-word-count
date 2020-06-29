package storm.word.count;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

  private static final Logger logger = LogManager.getLogger(WordCountTopology.class);

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    // builder.setSpout("spout", new RandomSentenceSpout(), 5);
    builder.setSpout("spout", new RandomSentenceSpout());

    // builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
    builder.setBolt("split", new SplitSentence()).shuffleGrouping("spout");

    // builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));
    builder.setBolt("count", new WordCount()).fieldsGrouping("split", new Fields("word"));

    Config conf = new Config();

    // Set to false to disable debug information when running in production on a cluster

    conf.setDebug(false);

    // If there are arguments, we are running on a cluster
    if (args != null && args.length > 0) {
      // parallelism hint to set the number of workers
      conf.setNumWorkers(3);
      // submit the topology
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    // Otherwise, we are running locally
    else {
      /*
       * Cap the maximum number of executors that can be spawned for a component to 3
       */
      // conf.setMaxTaskParallelism(3);

      // LocalCluster는 로컬에서 작동하는 클러스터
      LocalCluster cluster = new LocalCluster();

      // submit the topology
      cluster.submitTopology("WordCountTopology", conf, builder.createTopology());

      logger.info("TopologyBuilder Submit Topology");

      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }

      cluster.killTopology("WordCountTopology");

      cluster.shutdown();
    }
  }
}
