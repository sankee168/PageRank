package calculator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by sank on 10/14/16.
 */
public class GraphPropCal extends Configured {
    /**
     * job to calculate the graph properties
     * @param input input folder path
     * @param output output folder path
     * @throws IOException
     */
    public static void getGraph(String input, String output) throws IOException {
        JobConf jobConf = new JobConf(GraphPropCal.class);
        FileInputFormat.setInputPaths(jobConf, new Path(input));
        FileOutputFormat.setOutputPath(jobConf, new Path(output));
        jobConf.setMapperClass(GraphMapper.class);
        jobConf.setReducerClass(GraphReducer.class);
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(IntWritable.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);
        JobClient.runJob(jobConf);
    }


    public static class GraphMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        /**
         * reads the input and outputs the outdegrees for that node
         * @param key page
         * @param value pages
         * @param output
         * @param reporter
         * @throws IOException
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String input = value.toString();
            for (String line : input.split("\\r?\\n")) {
                int degree = 0;
                if (line.contains(" ")) {
                    String[] totalEdges = line.split("\\s+");
                    degree = totalEdges.length - 1;
                    output.collect(new Text("OutDegree"),
                            new IntWritable(degree));
                } else {
                    output.collect(new Text("OutDegree"), new IntWritable(0));
                }
            }
        }
    }

    public static class GraphReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
        /**
         * calcuates the all the graph properites here.
         * @param key page
         * @param values all the out degree
         * @param out
         * @param reporter
         * @throws IOException
         */
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
            int nodesCount = 0;
            float minDegree = Integer.MAX_VALUE, maxDegree = Integer.MIN_VALUE, degreeSum = 0, degree = 0;
            while (values.hasNext()) {
                IntWritable intWritable = values.next();
                degree = intWritable.get();
                if (maxDegree < degree) {
                    maxDegree = degree;
                }
                if (minDegree > degree) {
                    minDegree = degree;
                }
                degreeSum = degreeSum + degree;
                nodesCount++;
            }
            float avgDegree = degreeSum / nodesCount;
            out.collect(new Text("total nodes"), new Text(Integer.toString(nodesCount)));
            out.collect(new Text("total edges"), new Text(Float.toString(degreeSum)));
            out.collect(new Text("max degree"), new Text(Float.toString(maxDegree)));
            out.collect(new Text("min degree"), new Text(Float.toString(minDegree)));
            out.collect(new Text("average degree"), new Text(Float.toString(avgDegree)));
        }
    }
}
