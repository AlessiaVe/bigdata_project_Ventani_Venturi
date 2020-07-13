package second_job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * ArrestedPercentage class contains methods to complete the second task
 */
public class ArrestedPercentage {

    /**
     * FirstMapper takes in input a file with record composed by field separated by semicolon,
     * in output it returns (year__iucr__,arrested) like (2001__0110__,true)
     */
    public static class FirstMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text yearAndCrimeType = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] csv = value.toString().split(";");
            if(!csv[17].contains("Year")) {
                yearAndCrimeType.set(csv[17] + "__" + csv[4] + "__");
                context.write(yearAndCrimeType, new Text(csv[8]));
            } else {
                return;
            }
        }

    }

    /**
     * FirstReducer takes the output of FirstMapper and it calculates the arrested percentage,
     * output is (year__iucr__,percentageArrested) like (2001__0110__,100)
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
            context.write(key, new DoubleWritable(tot * 100 / count));
        }

    }

    /**
     * SecondMapper starts the second mapreduce job,
     * it maps the input year__iucr__percentageArrested into year,iucr percentageArrested like 2001,0110 100
     */
    public static class SecondMapper
            extends Mapper<Object, Text, LongWritable, Text>{

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] values = value.toString().split("__");
            context.write(new LongWritable(Integer.parseInt(values[0])), new Text(values[1]+" "+ values[2]));
        }

    }

    /**
     * SecondReducer writes the input received from SecondMapper
     */
    public static class SecondReducer
            extends Reducer<LongWritable,Text,LongWritable,Text> {

        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            while (values.iterator().hasNext()) {
                context.write(key, new Text(values.iterator().next().toString()));
            }
        }

    }


    public static void main(String[] args) throws Exception {

        // check argoment and file
        FileSystem fs = FileSystem.get(new Configuration());
        Path firstInputPath = new Path(args[0]),
                firstOutputPath = new Path(args[1]),
                partitionerFile = new Path(args[2]),
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
        firstJob.setNumReduceTasks(9);

        // set input and output files
        FileInputFormat.addInputPath(firstJob, firstInputPath);
        FileOutputFormat.setOutputPath(firstJob, firstOutputPath);

        int code = firstJob.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {

            // SECOND JOB
            Job secondJob = Job.getInstance(new Configuration(), "Order by Year");
            secondJob.setJarByClass(ArrestedPercentage.class);

            // Mapper configuration
            secondJob.setMapperClass(SecondMapper.class);
            secondJob.setMapOutputKeyClass(LongWritable.class);
            secondJob.setMapOutputValueClass(Text.class);

            // Reducer configuration
            secondJob.setReducerClass(SecondReducer.class);
            secondJob.setOutputKeyClass(LongWritable.class);
            secondJob.setOutputValueClass(Text.class);
            secondJob.setNumReduceTasks(9);

            // set input and output files
            FileInputFormat.setInputPaths(secondJob, firstOutputPath);
            FileOutputFormat.setOutputPath(secondJob, secondOutputPath);

            //create total order partitioner file
            secondJob.setPartitionerClass(TotalOrderPartitioner.class);
            TotalOrderPartitioner.setPartitionFile(secondJob.getConfiguration(), partitionerFile);
            InputSampler.Sampler<Text, Text> inputSampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);
            InputSampler.writePartitionFile(secondJob, inputSampler);
            secondJob.setPartitionerClass(TotalOrderPartitioner.class);
            secondJob.setPartitionerClass(TotalOrderPartitioner.class);

            code = secondJob.waitForCompletion(true) ? 0 : 1;
        }
        System.exit(code);

    }
}
