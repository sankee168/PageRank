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
public class AlterOutput extends Configured {
    /**
     * change the output for calculating the top pages
     * @param input input folder path
     * @param output output folder path
     * @throws IOException
     */
    public void print(String input, String output) throws IOException {
        JobConf jobConf = new JobConf(GraphPropCal.class);
        FileInputFormat.setInputPaths(jobConf, new Path(input));
        FileOutputFormat.setOutputPath(jobConf, new Path(output));
        jobConf.setMapperClass(OutputMapper.class);
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(Text.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);
        JobClient.runJob(jobConf);

    }

    /**
     * simple mapper which alters the input location so that top pages can be calculated easily.
     * collects page and rank in the output
     */
    public static class OutputMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
            String input = value.toString();
            for (String line : input.split("\\r?\\n")) {
                String[] temp = line.split("\\$");
                String nodes = temp[0];
                float rank = Float.parseFloat(temp[1]);
                String[] listOfNodes = nodes.split("\\s+");
                String node = listOfNodes[0];
                out.collect(new Text(node), new Text(Float.toString(rank)));
            }
        }
    }
}
