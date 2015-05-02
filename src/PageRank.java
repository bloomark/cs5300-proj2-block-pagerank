import java.io.IOException;
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
	 * sum=0; c_list=[]
	 * for line in open('blocks.txt'): 
	 * sum+=int(line.strip())
	 * c_list.append(sum) 
	 */
	public static int[] cumulative_block_list = {10328, 20373, 30629, 40645, 
		50462, 60841, 70591, 80118, 90497, 100501, 110567, 120945, 
		130999, 140574, 150953, 161332, 171154, 181514, 191625, 202004, 
		212383, 222762, 232593, 242878, 252938, 263149, 273210, 283473, 
		293255, 303043, 313370, 323522, 333883, 343663, 353645, 363929, 
		374236, 384554, 394929, 404712, 414617, 424747, 434707, 444489, 
		454285, 464398, 474196, 484050, 493968, 503752, 514131, 524510,
		534709, 545088, 555467, 565846, 576225, 586604, 596585, 606367,
		616148, 626448, 636240, 646022, 655804, 665666, 675448, 685230};
	
	/*
	 * Run the page rank job. Each run creates the output for the succeeding run, duh, so the output file
	 * for the `i`th run is <output>/iteration<i+1>. Counter class to get the residual error from each run.  
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2){
			System.out.println("Usage: <input> <output>");
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
