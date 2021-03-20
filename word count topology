
/*Implementing the sentence spout*/
public class SentenceSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private String[] sentences = {
			"my dog has fleas",
			"i like cold beverages",
			"the dog ate my homework",
			"don't have a cow man",
			"i don't think i like fleas"
	};
	private int index = 0;
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
	public void open(Map config, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}
	public void nextTuple() {
		this.collector.emit(new Values(sentences[index]));
		index++;
		if (index >= sentences.length) {
			index = 0;
		}
		Utils.waitForMillis(1);
	}
}

/*http://www.corejavaguru.com/bigdata/storm/word-count-topology*/

/*implment split sentence bolt*/
public class SplitSentenceBolt extends BaseRichBolt{
	private OutputCollector collector;
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}
	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for(String word : words){
			this.collector.emit(new Values(word));
		}
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}


/*Implementing the word count bolt*/

ublic class WordCountBolt extends BaseRichBolt{
	private OutputCollector collector;
	private HashMap<String, Long> counts = null;
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.counts = new HashMap<String, Long>();
	}
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = this.counts.get(word);
		if(count == null){
			count = 0L;
		}
		count++;
		this.counts.put(word, count);
		this.collector.emit(new Values(word, count));
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}
}

/*Implementing the report bolt*/
public class ReportBolt extends BaseRichBolt {
	private HashMap<String, Long> counts = null;
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.counts = new HashMap<String, Long>();
	}
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = tuple.getLongByField("count");
		this.counts.put(word, count);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// this bolt does not emit anything
	}
	public void cleanup() {
		System.out.println("--- FINAL COUNTS ---");
		List<String> keys = new ArrayList<String>();
		keys.addAll(this.counts.keySet());
		Collections.sort(keys);
		for (String key : keys) {
			System.out.println(key + " : " + this.counts.get(key));
		}
		System.out.println("--------------");
	}
}

/*Implementing the word count topology*/

public class WordCountTopology {
	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";
	public static void main(String[] args) throws Exception {
		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
		WordCountBolt countBolt = new WordCountBolt();
		ReportBolt reportBolt = new ReportBolt();
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SENTENCE_SPOUT_ID, spout);
		// SentenceSpout --> SplitSentenceBolt
		builder.setBolt(SPLIT_BOLT_ID, splitBolt)
		.shuffleGrouping(SENTENCE_SPOUT_ID);
		// SplitSentenceBolt --> WordCountBolt
		builder.setBolt(COUNT_BOLT_ID, countBolt)
		.fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
		// WordCountBolt --> ReportBolt
		builder.setBolt(REPORT_BOLT_ID, reportBolt)
		.globalGrouping(COUNT_BOLT_ID);
		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, builder.
				createTopology());
		waitForSeconds(10);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
	}
}
