package second_job;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 *
 */
public class ArrestedPercentage {


    /**
     * First Mapper
     */
    public static class FirstMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text yearAndCrimeType = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] csv = value.toString().split(";");
            yearAndCrimeType.set(csv[17]+"__"+csv[4]);

            context.write(yearAndCrimeType, new Text(csv[8]));
        }

    }

    /**
     * First Reducer
     */
    public static class FirstReducer
            extends Reducer<Text, Text, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double tot = 0, count = 0;
            while (values.iterator().hasNext()) {
                count++;
                if (values.iterator().next().toString().toLowerCase().equals("true")){
                    tot = tot + 1;
                }
            }
            context.write(key, new DoubleWritable(tot / count));
        }

    }

    /**
     * Second Mapper
     */
    public static class SecondMapper
            extends Mapper<Text, DoubleWritable, IntWritable, Text>{

        public void map(Text key, Double value, Context context)
                throws IOException, InterruptedException {
            String[] values = value.toString().split("__");
            context.write(new IntWritable(Integer.parseInt(values[0])),new Text(values[1]+","+ value));
        }

    }

    /**
     * Second Reducer
     */
    /*public static class SecondReducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Reducer logic

        }

    }*/


    public static void main(String[] args) throws Exception {

        // check argoment and file
        FileSystem fs = FileSystem.get(new Configuration());
        Path firstInputPath = new Path(args[0]),
                firstOutputPath = new Path(args[1]),
                partitionOutputPath = new Path(args[2]),
                secondOutputPath = new Path(args[3]);

        if (fs.exists(firstOutputPath)) {
            fs.delete(firstOutputPath, true);
        }
        if (fs.exists(secondOutputPath)) {
            fs.delete(secondOutputPath, true);
        }

        // FIRST JOB
        Configuration conf = new Configuration();
        Job firstJob = Job.getInstance(conf, "Count Percentage Arrested");
        firstJob.setMapOutputKeyClass(Text.class);
        firstJob.setMapOutputValueClass(Text.class);

        // set configuration
        firstJob.setJarByClass(ArrestedPercentage.class);
        firstJob.setMapperClass(FirstMapper.class);
        firstJob.setReducerClass(FirstReducer.class);
        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(DoubleWritable.class);

        // set input and output files
        FileInputFormat.addInputPath(firstJob, firstInputPath);
        FileOutputFormat.setOutputPath(firstJob, firstOutputPath);

        // SECOND JOB
        Job secondJob = Job.getInstance(new Configuration(), "Order by Year");
        secondJob.setJarByClass(ArrestedPercentage.class);

        // Mapper configuration
        secondJob.setMapperClass(SecondMapper.class);
        secondJob.setMapOutputKeyClass(Text.class);
        secondJob.setMapOutputValueClass(Text.class);

        // Reducer configuration
        //secondJob.setReducerClass(Reducer.class);
        secondJob.setOutputKeyClass(IntWritable.class);
        secondJob.setOutputValueClass(Text.class);
        secondJob.setNumReduceTasks(3);
        // Partitioning/Sorting/Grouping configuration
        secondJob.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(secondJob.getConfiguration(), partitionOutputPath);
        InputSampler.Sampler<Text, DoubleWritable> sampler = new InputSampler.RandomSampler<>(1,1);
        InputSampler.writePartitionFile(secondJob, sampler);

        // set input and output files
        FileInputFormat.setInputPaths(secondJob, firstOutputPath);
        FileOutputFormat.setOutputPath(secondJob, secondOutputPath);

        // EXECUTION OF THE TASKS
        ArrayList<Job> jobs = new ArrayList<>();

        jobs.add( firstJob );
        jobs.add( secondJob );

        for (Job job: jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }

    }
}
