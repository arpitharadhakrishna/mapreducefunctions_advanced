/**
 *@author arpita
 */
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FirstAnswer {

    

	
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		private Text userid = new Text();
		private Text gender = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("::");
			if (mydata[1].equals("F")) {
				userid.set(mydata[0]);
				gender.set(mydata[1]);
				context.write(userid, gender);
			}

		}
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		private Text userid = new Text();
		private Text movieidRating = new Text();
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("::");
			String movieID = mydata[1];
			String ratings = mydata[2];
			userid.set(mydata[0]);
			movieidRating.set(movieID + "~" + ratings);
			context.write(userid, movieidRating);
		}

	}

	public static class Reduce1 extends Reducer<Text, Text, Text, IntWritable> 
	{

		private Text key1 = new Text();
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			boolean isFemale = false;
			boolean hasRated = false;
			HashMap<String, String> mydata = new HashMap<String, String>();

			for (Text val : values) 
			{
				if (val.toString().equals("F")){
					isFemale = true;
				}
				else {
					try {
						
						String[] Mydata = val.toString().split("~");
						if(Mydata.length > 1){
						
							String movieID = Mydata[0];
							String ratings = Mydata[1];
							mydata.put(movieID, ratings);
							hasRated = true;
						}
					} 
					catch (NullPointerException e) 
					{
						continue;
					}
					
				}
		}
			if (isFemale && hasRated) 
			{
				
				Iterator<Entry<String, String>> it = mydata.entrySet().iterator();
				while (it.hasNext())
				{
					Map.Entry pair = it.next();
					key1.set((String)pair.getKey());
					result.set((int)Integer.parseInt((String)pair.getValue()));
					context.write(key1, result);
				}
			}
			isFemale = false;
			hasRated = false;
	}

}
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> 
	{
		Text keys=new Text();
		Text values=new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] val=value.toString().split("\\s+");
			keys.set(val[0]);
            values.set(val[1]);
            context.write(keys, values);
		}
	}
	
	public static class Map4 extends Mapper<LongWritable, Text, Text, Text> 
	{
		private Text moviesid = new Text();
		private Text movies_title = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] mydata = value.toString().split("::");
			moviesid.set(mydata[0]);
			movies_title.set(mydata[1]);
			context.write(moviesid, movies_title);
		}
	}

	public static class ValueComparator implements Comparator<String> 
	{

		Map<String, Float> base;

		public ValueComparator(Map<String, Float> base) 
		{
			this.base = base;
		}

		// Note: this comparator imposes orderings that are inconsistent with
		// equals.
		public int compare(String a, String b) 
		{
			if (base.get(a) >= base.get(b))
			{
				return -1;
			} 
			else
			{
				return 1;
			}
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		private Text moviesTitle = new Text();
		private Text ratings = new Text();
		HashMap<String, Float> myMap = new HashMap<String, Float>();
		ValueComparator bvc = new ValueComparator(myMap);
		boolean movieflag = false;
		boolean argflag = false;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String movieTitle = new String();
			String abc=new String();
			String dec=new String();
		    Float sum =0.0f,avg=0.0f;
			
            int count=0;
		     for(Text val:values )
		     {
		    		 if(val.toString().length()==1)
		    		 {
		    		   dec=val.toString();
					   count++; 
					   sum += Float.parseFloat(dec);
					   argflag=true;
					}
		    		 else
			     	{
					movieflag=true;
					movieTitle=val.toString();
					moviesTitle.set(movieTitle);
				    }
				
					if (count>0)
					{
						avg=(float)sum/count;
						ratings.set(avg+"");
						//context.write(moviesTitle, ratings);
						myMap.put(movieTitle, avg);
					}
		     }
		}

		protected void cleanup(Context context) throws IOException, InterruptedException 
		{
			TreeMap<String, Float> sorted_map = new TreeMap<String, Float>(bvc);
			sorted_map.putAll(myMap);
			int top = 0;
			Iterator it = sorted_map.entrySet().iterator();

			while (it.hasNext()) 
			{
				if (top == 5)
					break;
				Map.Entry pair = (Map.Entry) it.next();
				moviesTitle.set(pair.getKey().toString());
				ratings.set(pair.getValue().toString());
				context.write(moviesTitle, ratings);
				top++;
				it.remove();
			}
	}

}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		
		if (otherArgs.length != 5) {
			System.err.println("output format: users.dat ratings.dat movies.dat intermediateoutputFolder outputFolder");
			System.exit(2);
		}
	
		Job job1 = new Job(conf1, "joinexc1");

		job1.setJarByClass(FirstAnswer.class);
		job1.setReducerClass(Reduce1.class);
		
		MultipleInputs.addInputPath(job1, new Path(otherArgs[0]), TextInputFormat.class, Map1.class);

		MultipleInputs.addInputPath(job1, new Path(otherArgs[1]), TextInputFormat.class, Map2.class);
		
		job1.setOutputKeyClass(Text.class);
		
		job1.setOutputValueClass(Text.class);

		
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[3]));

		job1.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "joinexc2");

		job2.setJarByClass(FirstAnswer.class);
		job2.setReducerClass(Reduce2.class);

		MultipleInputs.addInputPath(job2, new Path(otherArgs[3]), TextInputFormat.class, Map3.class);
		MultipleInputs.addInputPath(job2, new Path(otherArgs[2]), TextInputFormat.class, Map4.class);
		
		job2.setOutputKeyClass(Text.class);
	
		job2.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));
		job2.waitForCompletion(true);
		
		try{}
		finally {
			FileSystem fs = FileSystem.get(conf2);
			fs.delete(new Path(otherArgs[3]), true);

		}
	}
}
