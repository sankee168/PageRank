package calculator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * Created by sank on 10/14/16.
 */

/**
 * This job calcualates the top pages based on the rank.
 */
public class TopPagesCal extends Configured {
    public static void topPages(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = new Job(configuration, "topPagesCal");
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setMapperClass(TopPageMapper.class);
        job.setReducerClass(TopPageReducer.class);
        job.setNumReduceTasks(1);
        job.setJarByClass(TopPagesCal.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }

    public static class TopPageMapper extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * just reads the input from the file and throws at reducer
         * @param key page
         * @param value rank
         * @param context Context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString();
            for (String line : input.split("\\r?\\n")) {
                String[] nodeswithRank = line.split("\\s+");
                float pageRank = Float.parseFloat(nodeswithRank[1]);
                String pageNode = nodeswithRank[0];
                context.write(new Text(pageNode), new Text(Float.toString(pageRank)));
            }
        }
    }

    public static class TopPageReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * stores the map as the uber variable to store top 10 pages
         */
        Map<String, Float> finalMap = new HashMap<String, Float>();

        /**
         * takes all the pages as input and stores the top 10 in finalMap
         * @param key page
         * @param values rank
         * @param context
         * @throws IOException
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
            Iterator<Text> iter = values.iterator();
            while (iter.hasNext()) {
                Float rank = Float.parseFloat(iter.next().toString());
                String page = key.toString();
                finalMap.put(page, rank);
                if (finalMap.size() > 10) {
                    String minimum = getMinFromMap(finalMap);
                    finalMap.remove(minimum);
                }
            }
        }

        /**
         * prints the map to the output collector
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void cleanup(Context context) throws IOException, InterruptedException {
            Map<String, Float> tempMap = finalMap;
            while (tempMap.size() != 0) {
                String key = getMaxFromMap(tempMap);
                Text value = new Text(finalMap.get(key).toString());
                context.write(new Text(key), value);
                tempMap.remove(key);
            }
        }


        /**
         * gets the page with rank value
         * @param pageMap map of page to rank
         * @return page with max rank
         */
        private String getMaxFromMap(Map<String, Float> pageMap) {
            Iterator<String> iter = pageMap.keySet().iterator();
            String max = "";
            while (iter.hasNext()) {
                String currKey = iter.next();

                if (max.length() == 0) {
                    max = currKey;
                }
                if (pageMap.get(max) < pageMap.get(currKey)) {
                    max = currKey;
                }
            }
            return max;
        }

        /**
         * gets the page with min rank value
         * @param pageMap map of page and rank
         * @return page with min rank value
         */
        private String getMinFromMap(Map<String, Float> pageMap) {
            Iterator<String> iter = pageMap.keySet().iterator();
            String min = "";
            while (iter.hasNext()) {
                String currKey = iter.next();

                if (min.length() == 0) {
                    min = currKey;
                }
                if (pageMap.get(min) > pageMap.get(currKey)) {
                    min = currKey;
                }
            }
            return min;
        }
    }
}


