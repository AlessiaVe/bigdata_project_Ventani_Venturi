import java.io.IOException;
import java.util.StringTokenizer;
import java.io.StringReader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.util.ArrayList;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class CrimesCountDistrictJoin {


	// FIRST JOB: COUNT OF CRIMES BY DISTRICT
	public static class TokenizerMapper
			extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text discrict_primary_type = new Text();

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			{
				String[] csv = value.toString().split(";");
				discrict_primary_type.set(csv[9]+";"+csv[4]+"__");
				// delete some error in the data
				if(!csv[9].equals("true") && !csv[9].equals("false") && !csv[9].equals("District") && !csv[9].contains(" ")) {
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
			// OUTPUT (DISTRIC, (IURC, TOTAL NUMBER))
			context.write(key, result);
		}
	}

	// JOIN WITH THE TABLE OF IURC
	public static class FirstMapperJoin
			extends Mapper<Object, Text, Text, Text>{

		private Text IURCcode = new Text();
		private Text description = new Text();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// ouput (IURC, (DISTRICT, TOTAL NUMBER))
			String[] values = value.toString().split("__");
			description.set("DX" + key.toString() + "__" + values[1].replaceAll("\\s", "")+"__");
			IURCcode.set(values[0].replaceAll("\\s", ""));
			context.write(IURCcode, description);
		}

	}

	// MAPPER FOR THE TABLE OF IURC
	public static class SecondMapperJoin
			extends Mapper<Object, Text, Text, Text>{

		private Text IURCcode = new Text();
		private Text description = new Text();

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			{
                // output (IURC, DESCRIPTION)
				String[] csv = value.toString().split(";");
				if(key.toString().equals("IUCR")){
					return;
				}else{
					IURCcode.set(key.toString());
					description.set("SX"+csv[1]);
					context.write(IURCcode, description);
				}
			}
		}

	}

	// JOIN REDUCER
	public static class JoinReducer
			extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			List<Text> firstDatasetRecords = new ArrayList<Text>();
			List<Text> secondDatasetRecords = new ArrayList<Text>();

			for(Text val : values) {
				if(val.toString().startsWith("DX")){
					firstDatasetRecords.add(new Text(val.toString().substring(2)));
				}else if (val.toString().startsWith("SX")){
					secondDatasetRecords.add(new Text(val.toString().substring(2)));
				}

			}

			for(Text first : firstDatasetRecords) {
				for(Text second : secondDatasetRecords) {
					String[] firstPart = first.toString().split("__");
					Text finalValue = new Text();
					Text finalKey = new Text();
					finalKey.set(firstPart[0]);
					finalValue.set("££"+second+"££"+firstPart[1]);
					// OUTPUT (DISTRIC, (DESCRIPTION, TOTAL NUMBER))
					context.write(finalKey, finalValue);

				}
			}
		}

	}


	// SECOND JOB: ORDER BY DISCRICT (ASC) AND CRIMES FOR TOTAL NUMBER (DESC)
	public static class CompositeKeyCreationMapper extends Mapper<Object, Text, CompositeKey, Text> {

		private CompositeKey compositeKey = new CompositeKey();
		private Text description = new Text();
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split("££");
			description.set(values[1]);
			compositeKey.set(values[0].replaceAll("\\s",""), Integer.parseInt(values[2].replaceAll("\\s","")));
			context.write(compositeKey, description);


		}

	}

	// SECOND REDUCER
	public static class ValueOutputReducer extends Reducer<CompositeKey, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void reduce(CompositeKey key, Iterable<Text> descriptions, Context context) throws IOException, InterruptedException {

			for (Text description : descriptions){
				outputKey.set(key.district);
				outputValue.set(description+" "+key.count);
				context.write(outputKey, outputValue);
			}
		}
	}


	public static void main(String[] args) throws Exception {

		// check argoment and file
		FileSystem fs = FileSystem.get(new Configuration());
		Path firstInputPath = new Path(args[0]),
				firstOutputPath = new Path(args[1]),
				IURCFilePath = new Path(args[2]),
				secondOutputPath = new Path(args[3]),
				outputPath = new Path(args[4]);

		if (fs.exists(firstOutputPath)) {
			fs.delete(firstOutputPath, true);
		}
		if (fs.exists(secondOutputPath)) {
			fs.delete(secondOutputPath, true);
		}
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}


		// FIRST JOB
		Configuration conf = new Configuration();
		Job firstJob = Job.getInstance(conf, "OrderCrimes");

		firstJob.setMapOutputKeyClass(Text.class);
		firstJob.setMapOutputValueClass(IntWritable.class);

		firstJob.setJarByClass(CrimesCountDistrictJoin.class);
		firstJob.setMapperClass(TokenizerMapper.class);

		firstJob.setCombinerClass(IntSumReducer.class);
		firstJob.setReducerClass(IntSumReducer.class);
		firstJob.setOutputKeyClass(Text.class);
		firstJob.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(firstJob, firstInputPath);
		FileOutputFormat.setOutputPath(firstJob, firstOutputPath);

		// JOIN

		Configuration joinConf = new Configuration();
		// set separator from key and value in line of the file
		joinConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ";");
		Job joinJob = Job.getInstance(joinConf, "Join job");
		joinJob.setJarByClass(CrimesCountDistrictJoin.class);
		joinJob.setMapOutputKeyClass(Text.class);
		joinJob.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(joinJob, secondOutputPath);

		MultipleInputs.addInputPath(joinJob, firstOutputPath, KeyValueTextInputFormat.class,  FirstMapperJoin.class);
		MultipleInputs.addInputPath(joinJob, IURCFilePath, KeyValueTextInputFormat.class, SecondMapperJoin.class);

		joinJob.setReducerClass(JoinReducer.class);


		// SECOND JOB
		Job secondJob = Job.getInstance(new Configuration(), "Secondary Sorting");
		secondJob.setJarByClass(CrimesCountDistrictJoin.class);

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

		FileInputFormat.setInputPaths(secondJob, secondOutputPath);
		FileOutputFormat.setOutputPath(secondJob, outputPath);


		// EXECUTION OF THE TASKS
		ArrayList<Job> jobs = new ArrayList<>();

		jobs.add( firstJob );
		jobs.add( joinJob );
		jobs.add( secondJob );

		for (Job job: jobs) {
			if (!job.waitForCompletion(true)) {
				System.exit(1);
			}
		}


	}
}

