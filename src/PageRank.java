import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;

import pagerankcounter.PageRankCounterClass.PageRankCounter;

public class PageRank {
	public static int NUM_NODES = 685230;
	
	/*
	 * Run the page rank job. Each run creates the output for the succeeding run, duh, so the output file
	 * for the `i`th run is <output>/iteration<i+1>. Counter class to get the residual error from each run.  
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 3){
			System.out.println("Usage: <input> <output> <result_file>");
			System.exit(1);
		}
		
		List<Double> residueList = new ArrayList<Double>();
		
		residueList.add(runPageRank(args[0], args[1] + "/iteration1", 0));
		
		for(int i=1; i<5; i++)
			residueList.add(runPageRank(
					args[1] + "/iteration" + String.valueOf(i), //Input File 
					args[1] + "/iteration" + String.valueOf(i+1), //Output File
					i)); //Iteration
		
		// Print the residual value
		for(Double residue:residueList)
			System.out.println("Residual Value = " + residue/(NUM_NODES * 10e5));
		
		PrintWriter writer = new PrintWriter(args[2], "UTF-8");
		for(Double residue:residueList)
			writer.println("Residue Value = " + residue/NUM_NODES);
		writer.close();
	}

	public static Double runPageRank(String input, String output, int i) throws IOException, ClassNotFoundException, InterruptedException{
		// Self explanatory config stuff for the MapReduce job
		Job job = Job.getInstance(new Configuration());
		job.setJobName("pr_" + String.valueOf(i));
		job.setJarByClass(PageRank.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		
		// Get the residual value from the counter and reset it for the next iteration!
		Double residue = (double) job.getCounters().findCounter(PageRankCounter.RESIDUAL).getValue();
		job.getCounters().findCounter(PageRankCounter.RESIDUAL).setValue(0L);
		return residue; 
	}
}
