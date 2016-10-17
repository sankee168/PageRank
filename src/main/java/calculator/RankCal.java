package calculator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by sank on 10/14/16.
 */
public class RankCal extends Configured{

    public static float factor;
    public static int nodes = 0;

    public enum Counter {
        counter;
    }

    /**
     * calculates the page rank
     * @param inputPath input folder path
     * @param outputPath output folder path
     * @param factor damping factor
     * @return
     * @throws Exception
     */
    public long calculatePageRank(String inputPath, String outputPath, String factor) throws Exception {
        JobConf jobConf = new JobConf(new Configuration(), GraphPropCal.class);
        FileInputFormat.setInputPaths(jobConf, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));

        jobConf.setMapperClass(RankMapper.class);
        jobConf.setReducerClass(RankReducer.class);

        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(Text.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);
        jobConf.set("DAMPINGFACTOR", factor);
        RunningJob job = JobClient.runJob(jobConf);

        Counters counters = job.getCounters();
        long nonconvergentnodes = counters.getCounter(Counter.counter);
        return nonconvergentnodes;
    }


    public static class RankMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        /**
         * reads the input from the input location
         * @param key
         * @param value
         * @param output
         * @param reporter
         * @throws IOException
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String input = value.toString();
            for (String line : input.split("\\r?\\n")) {
                String[] nodeswithRank = line.split("\\$");
                String nodes = nodeswithRank[0];
                float rank = Float.parseFloat(nodeswithRank[1]), rankShare = (float) 0.0;
                String[] nodeSet = nodes.split("\\s+");
                String mainNode = nodeSet[0];
                String remEdges = nodes.replace(mainNode, "");
                int length = nodeSet.length;
                if (length > 1) {
                    rankShare = rank / (length - 1);
                    for (String node : nodeSet) {
                        if (!node.equals(mainNode)) {
                            output.collect(new Text(node), new Text(Float.toString(rankShare)));
                        }
                    }
                }
                output.collect(new Text(mainNode), new Text(remEdges + "$" + rank + "~"));
            }
        }
    }

    public static class RankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        /**
         * spreads the influence to the pages connected and calculates the rank
         * @param key page
         * @param values rank from others
         * @param output
         * @param reporter
         * @throws IOException
         */
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            float combined = (float) 0.0;
            float share = (float) 0.0;
            float rank = (float) 0.0;
            String nodes = "";
            while (values.hasNext()) {
                Text text = values.next();
                String currNode = text.toString();
                if (currNode.contains("~")) {
                    String[] rankedNodes = currNode.split("\\$");
                    nodes = rankedNodes[0];
                    rank = Float.parseFloat(rankedNodes[1].replace("~", ""));
                } else {
                    share = Float.parseFloat(currNode);
                    combined = combined + share;
                }
            }
            float rank1 = (float) ((1 - factor) + (factor * combined));
            if (Math.abs(rank - rank1) > 0.001) {
                reporter.incrCounter(Counter.counter, 1);
            }
            output.collect(key, new Text(nodes + "$" + Float.toString(rank1)));
        }

        public void configure(JobConf jobConf) {
            factor = Float.parseFloat(jobConf.get("DAMPINGFACTOR"));
        }
    }
}
