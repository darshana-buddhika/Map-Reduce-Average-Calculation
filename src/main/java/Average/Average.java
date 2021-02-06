package Average;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
                word.set(String.valueOf(getNumber(10)));
                context.write(word, new IntWritable(Integer.parseInt(itr.nextToken())));
            }
        }
    }

    public static class SumReducer extends Reducer<Text,IntWritable,Text, LongWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            int count = 0;
            System.out.println("the key is "+ key);
            for (IntWritable val : values) {
                sum += val.get();
                System.out.println(sum);
                count++;
            }
            context.write(new Text(String.valueOf(count)), new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average");
        job.setJarByClass(Average.class);
        job.setMapperClass(NumberMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
