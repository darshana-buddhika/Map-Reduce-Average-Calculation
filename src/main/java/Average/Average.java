package Average;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Average {
    private static Random random = new Random();

    public static int getNumber(int numberOfReducers) {
        return random.nextInt(numberOfReducers);

    }

    public static class NumberMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(String.valueOf(getNumber(20)));
                context.write(word, new IntWritable(Integer.parseInt(itr.nextToken())));
            }
        }
    }

    public static class SumReducer extends Reducer<Text,IntWritable,Text, LongWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            context.write(new Text(String.valueOf(count)), new LongWritable(sum));
        }
    }

    public static class OutputMapper extends Mapper<Object, Text, Text, MapWritable> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            MapWritable map = new MapWritable();
            String[] line = value.toString().split("\t");
            map.put(new LongWritable(Long.parseLong(line[0])), new LongWritable(Long.parseLong(line[1])));

            context.write(new Text(String.valueOf(1)), map);

        }
    }

    public static class FinalReducer extends Reducer<Text, MapWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            int count = 0;
            for (MapWritable val : values) {
                Set<Entry<Writable, Writable>> entrySet = val.entrySet();
                for (Entry<Writable, Writable> entry : entrySet) {
                    count += Long.parseLong(entry.getKey().toString());
                    sum += Long.parseLong(entry.getValue().toString());
                }
            }

            float average = (float) sum/count;
            System.out.println(average);
            context.write(new Text("Average"), new FloatWritable(average));
        }
    }

    public static void main(String[] args) throws Exception {
        final String OUTPUT_PATH = "intermediate_output";

        // Job 1 : splitting the numbers and getting partial sum and count
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "job1");
        job1.setJarByClass(Average.class);
        job1.setMapperClass(NumberMapper.class);
        job1.setReducerClass(SumReducer.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));

        job1.waitForCompletion(true);

        // Job 2 : getting the partial sum and count as input and calculating the average
        Job job2 = Job.getInstance(conf, "job2");
        job2.setJarByClass(Average.class);
        job2.setMapperClass(OutputMapper.class);
        job2.setReducerClass(FinalReducer.class);
        job2.setMapOutputValueClass(MapWritable.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
