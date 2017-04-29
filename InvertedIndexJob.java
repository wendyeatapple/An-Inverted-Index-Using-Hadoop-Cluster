import java.io.IOException;
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

import java.util.HashMap;
import java.util.Iterator;

public class InvertedIndexJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: InvertedIndexJob <input path> <output path>");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "InvertedIndexJob");
		
		job.setJarByClass(InvertedIndexJob.class);
	    job.setJobName("InvertedIndexJob");

	    job.setMapperClass(InvertedIndexMapper.class);
	    job.setCombinerClass(InvertedIndexReducer.class);
	    job.setReducerClass(InvertedIndexReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
  
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	  
	}

}
class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text word = new Text();
	private final static Text document = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		String documentName = null;
		if (itr.hasMoreTokens()) {
			documentName = itr.nextToken();
		} 
		document.set(documentName);	
      	while (itr.hasMoreTokens()) {
        	word.set(itr.nextToken());
        	context.write(word, document);
      	}	
	}
}
class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws java.io.IOException, InterruptedException {
		HashMap m = new HashMap();
		int count = 0;
	
		for (Text value : values) {
			String str = value.toString();
			if (m != null && m.get(str) != null) {
				count = (int)m.get(str) + 1;
				m.put(str, count);
			} else {
				m.put(str, 1);
			}
		}
		StringBuilder s = new StringBuilder();
		Iterator it = m.entrySet().iterator();
	    while (it.hasNext()) {
	        HashMap.Entry pair = (HashMap.Entry) it.next();
	        s.append(pair.getKey() + ":" + pair.getValue() + "  ");
	        it.remove(); // avoids a ConcurrentModificationException
	    }
		context.write(key, new Text(s.toString()));
	}
}