import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;

//http://www.apache.org/dyn/closer.cgi/logging/log4j/1.2.17/log4j-1.2.17.tar.gz
import org.apache.log4j.Logger;

import pagerankcounter.PageRankCounterClass.PageRankCounter;

public class PageRankReducer extends Reducer<Text, Text, Text, Text>{
	private static double DAMPING_FACTOR = 0.85;
	private static double CONSTANT_VALUE = (1-DAMPING_FACTOR)/PageRank.NUM_NODES;
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Iterator<Text> iter = values.iterator();
		Double page_ranks_to_add = 0.0;
		
		Integer node_id = Integer.parseInt(key.toString());
		Double page_rank = 0.0;
		Integer degree = 0;
		String neighbors = "";
		
		while(iter.hasNext()){
			Text input = iter.next();
			
			String line = input.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			System.out.println("Line = " + line);
			
			if(tokenizer.countTokens() == 1){
				// Input was <node_id> <page_rank/degree>
				page_ranks_to_add += Double.parseDouble(tokenizer.nextToken());
			}
			else{
				// Input was <node_id> <page_rank> <degree> [<neighbour1> <neighbor2>....]
				node_id = Integer.parseInt(tokenizer.nextToken());
				page_rank = Double.parseDouble(tokenizer.nextToken());
				degree = Integer.parseInt(tokenizer.nextToken());
				neighbors = "";
				
				for(int i=0; i<degree; i++){
					neighbors = neighbors + tokenizer.nextToken() + " ";
				}
				neighbors = neighbors.trim();
			}
		}
		
		Double new_page_rank = CONSTANT_VALUE + DAMPING_FACTOR * page_ranks_to_add;
		
		// Recreate mapper input so the reducer can provide meaningful input to the mapper at the start
		// of the next job.
		Text key_to_emit = new Text(node_id.toString());
		String text_value_to_emit = new_page_rank.toString() + " " + degree.toString() + " " + neighbors;
		Text value_to_emit = new Text(text_value_to_emit.trim());
		
		// Update the counter with the residual error.
		Double residual_error = Math.abs(new_page_rank - page_rank)/new_page_rank;
		Long long_residual_error = (long) (residual_error * 10e5);
		context.getCounter(PageRankCounter.RESIDUAL).increment(long_residual_error);
		
		context.write(key_to_emit, value_to_emit);
	}
}
