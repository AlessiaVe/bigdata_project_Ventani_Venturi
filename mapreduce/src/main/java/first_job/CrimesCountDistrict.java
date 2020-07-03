import java.io.IOException;
import java.util.StringTokenizer;
import java.io.StringReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;


public class CrimesCountDistrict {

	// FIRST JOB: COUNT OF CRIMES BY DISTRICT
	public static class TokenizerMapper
			extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text discrict_primary_type = new Text();

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			{
				String[] csv = value.toString().split(";");
				discrict_primary_type.set(csv[11]+"__"+csv[6]+"__"+csv[4]+"__");
				// delete some error in the data
				if(!csv[11].equals("true") && !csv[11].equals("false") && !csv[11].equals("District") && !csv[11].contains(" ")  && !csv[6].contains("\"THEFT BY LESSEE")) {
					context.write(discrict_primary_type, one);
				}else{
					return;
				}

			}
		}
	}


	// FIRST REDUCER
	public static class IntSumReducer
			extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
						   Context context
		) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	// SECOND JOB: ORDER BY DISCRICT (ASC) AND CRIMES FOR TOTAL NUMBER (DESC)
	public static class CompositeKeyCreationMapper extends Mapper<Object, Text, CompositeKey, Text> {

		private CompositeKey compositeKey = new CompositeKey();
		private Text description = new Text();
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split("__");
			description.set(values[1]);
			compositeKey.set(values[0], Integer.parseInt(values[3].replaceAll("\\s","")));
			context.write(compositeKey, description);
		}

	}

	// SECOND REDUCER
	public static class ValueOutputReducer extends Reducer<CompositeKey, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void reduce(CompositeKey key, Iterable<Text> descriptions, Context context) throws IOException, InterruptedException {

			for (Text description : descriptions) {
				outputKey.set(key.district+"@");
				outputValue.set(description+"@"+key.count);
				context.write(outputKey, outputValue);
			}
		}
	}



	public static void main(String[] args) throws Exception {

		// check argoment and file
		FileSystem fs = FileSystem.get(new Configuration());
		Path firstInputPath = new Path(args[0]),
				firstOutputPath = new Path(args[1]),
				secondOutputPath = new Path(args[2]);

		if (fs.exists(firstOutputPath)) {
			fs.delete(firstOutputPath, true);
		}
		if (fs.exists(secondOutputPath)) {
			fs.delete(secondOutputPath, true);
		}

		// FIRST JOB
		Configuration conf = new Configuration();
		Job firstJob = Job.getInstance(conf, "OrderCrimes");
		firstJob.setMapOutputKeyClass(Text.class);
		firstJob.setMapOutputValueClass(IntWritable.class);

		// set configuration
		firstJob.setJarByClass(CrimesCountDistrict.class);
		firstJob.setMapperClass(TokenizerMapper.class);
		firstJob.setCombinerClass(IntSumReducer.class);
		firstJob.setReducerClass(IntSumReducer.class);
		firstJob.setOutputKeyClass(Text.class);
		firstJob.setOutputValueClass(IntWritable.class);

		// set input and output files
		FileInputFormat.addInputPath(firstJob, firstInputPath);
		FileOutputFormat.setOutputPath(firstJob, firstOutputPath);

		// SECOND JOB
		Job secondJob = Job.getInstance(new Configuration(), "Secondary Sorting");
		secondJob.setJarByClass(CrimesCountDistrict.class);

		// Mapper configuration
		secondJob.setMapperClass(CompositeKeyCreationMapper.class);
		secondJob.setMapOutputKeyClass(CompositeKey.class);
		secondJob.setMapOutputValueClass(Text.class);

		// Partitioning/Sorting/Grouping configuration
		secondJob.setPartitionerClass(NaturalKeyPartitioner.class);
		secondJob.setSortComparatorClass(FullKeyComparator.class);
		secondJob.setGroupingComparatorClass(NaturalKeyComparator.class);

		// Reducer configuration
		secondJob.setReducerClass(ValueOutputReducer.class);
		secondJob.setOutputKeyClass(Text.class);
		secondJob.setOutputValueClass(Text.class);
		secondJob.setNumReduceTasks(1);

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

