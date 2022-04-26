package maximum_user_frequency;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaximumUserFrequency{
	
	static class User implements Writable, WritableComparable<User> {
		private String name = "";
		private Integer freq = new Integer(0);
		
		public void setName(String n)
		{
			name = n;
		}
		
		public void setFreq(Integer f)
		{
			freq = f;
		}
		
		public String getName()
		{
			return name;
		}
		
		public Integer getFreq()
		{
			return freq;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			freq = in.readInt();
			name = in.readLine();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(freq);
			out.writeBytes(name);
		}

		@Override
		public int compareTo(User o) {
			// TODO Auto-generated method stub
			int result = this.freq.compareTo(o.freq);
			return result;
		}
	}
	
	private static User u = new User();

	static class UserMaxCountMapper extends Mapper<Object, Text, Text, User> {
		private Text user = new Text();
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			user.set("user");
			u.setName(value.toString().split(",")[0]);
			u.setFreq(Integer.parseInt(value.toString().split(",")[1]));
			context.write(user, u);
		}
	}

	static class UserMaxCountReducer extends Reducer<Text, User, Text, IntWritable> {
		private User result = new User();
		
		public void reduce(Text key, Iterable<User> values, Context context)
			throws IOException, InterruptedException {
			result.setFreq(null);
			result.setName("");
			
			for (User value : values) 
			{	
				if (result.getFreq() == null || (value.getFreq() > result.getFreq())) {
					result.setFreq(value.getFreq());
					result.setName(value.getName());
				}			
		    }
			
			key.set(result.getName());
		    context.write(key, new IntWritable(result.getFreq()));
		}
	}

	
	public static void main(String[] args) throws Exception {		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MaximumUserFrequency.class);
		job.setJobName("find_max_user_count");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(UserMaxCountMapper.class);
		job.setReducerClass(UserMaxCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(User.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
