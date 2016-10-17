package calculator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by sank on 10/14/16.
 */
public class AlterInput extends Configured {
    /**
     * job to alter the input
     * @param input input folder location
     * @param output output folder location
     * @throws IOException
     */
    public void alter(String input, String output) throws IOException {
        JobConf jobConf = new JobConf(GraphPropCal.class);
        FileInputFormat.setInputPaths(jobConf, new Path(input));
        FileOutputFormat.setOutputPath(jobConf, new Path(output));
        jobConf.setMapperClass(InputMapper.class);
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(Text.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);
        JobClient.runJob(jobConf);
    }


    public static class InputMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        /**
         * mapper to alter the input for rank calculation
         * @param key page
         * @param value rank
         * @param out
         * @param reporter
         * @throws IOException
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
            String input = value.toString();
            String startRank = "1";
            for (String line : input.split("\\r?\\n")) {
                if (line.contains("\\s+")) {
                    String node = line.substring(0, line.indexOf("\\s+"));
                    String remainingEdges = line.replace(node, "");
                    while (remainingEdges.startsWith("\\s+")) {
                        remainingEdges = remainingEdges.substring(1);
                    }
                    out.collect(new Text(node), new Text(remainingEdges + "$" + startRank));
                } else {
                    out.collect(new Text(line), new Text("$" + startRank));
                }
            }
        }
    }
}
