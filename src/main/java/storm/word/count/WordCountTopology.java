package storm.word.count;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    /*
     * spout id: 'spout', IRichSpout: storm.word.count.RandomSentenceSpout, parallelism hint: 5
     */
    builder.setSpout("spout", new RandomSentenceSpout(), 5);

    /*
     * bolt id: 'split', IBasicBolt: storm.word.count.SplitSentence, parallelism hint: 8,
     * shuffleGrouping: 'spout'
     */
    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");

    /*
     * bolt id: 'count', IBasicBolt: storm.word.count.WordCount, parallelism hint: 12,
     * fieldsgrouping subscribes to the split bolt, and // ensures that the same word is sent to the
     * same instance (group by field 'word')
     */
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

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
      conf.setMaxTaskParallelism(3);

      // LocalCluster는 로컬에서 작동하는 클러스터
      LocalCluster cluster = new LocalCluster();

      // submit the topology
      cluster.submitTopology("local cluster apache storm word-count", conf,
          builder.createTopology());

      cluster.shutdown();
    }
  }
}
