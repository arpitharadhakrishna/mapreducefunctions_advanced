/**
 * @author arpita
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SecondAnswer {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		String mid;
		HashMap<String, String> userMap = new HashMap<String, String>();

		public void setup(Context context) throws IOException, InterruptedException {

			String readLine;
			Configuration conf = context.getConfiguration();
			mid = conf.get("mid");
			FileSystem fs = FileSystem.getLocal(conf);
			Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
			BufferedReader cacheReader = new BufferedReader(new InputStreamReader(fs.open(localFiles[0])));
			String[] inputData;
			while ((readLine = cacheReader.readLine()) != null) {
				inputData = readLine.split("::");
				userMap.put(inputData[0], inputData[1] + "#" + inputData[2]);
			}
		}

		private Text res = new Text();
		private Text uid = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String s[];
			s = value.toString().split("::");
			String userDetails = null;
			if (s[1].equals(mid) && Integer.parseInt(s[2]) >= 4) {
				userDetails = userMap.get(s[0]);
				uid.set(s[0]);
				res.set(userDetails);
				context.write(uid, res);
			}

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				String[] reducedata = val.toString().split("#");
				val.set(reducedata[0] + " " + reducedata[1]);
				context.write(key, val);
			}

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: Q2 <in:ratings.dat> <out> <movie_id>");
			System.exit(2);
		}

		conf.set("mid", otherArgs[2]);

		DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/users/users.dat"), conf);

		Job job = new Job(conf, "JoinExec");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(SecondAnswer.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.waitForCompletion(true);
	}
}
