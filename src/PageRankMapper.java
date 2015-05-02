import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//http://www.apache.org/dyn/closer.cgi/logging/log4j/1.2.17/log4j-1.2.17.tar.gz
import org.apache.log4j.Logger;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text>{
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		// Input to the mapper is <block_id> <node_id> <page_rank> <out_degree> [<neighbor1> <neighbor2>....]
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		Integer block_id = Integer.parseInt(tokenizer.nextToken());
		Integer node_id  = Integer.parseInt(tokenizer.nextToken());
		Double page_rank = Double.parseDouble(tokenizer.nextToken());
		Integer degree   = Integer.parseInt(tokenizer.nextToken());
		
		Vector<Integer> neighbors = new Vector<Integer>();
		
		// Generate a vector of all neighbors
		for(int i=0; i<degree; i++){
			neighbors.add(Integer.parseInt(tokenizer.nextToken()));
		}
		
		if(degree > 0){
			Double double_value_to_emit = page_rank/degree;
			String temp_value_to_emit_str = getBlockId(node_id).toString() + " " + double_value_to_emit.toString();
		
			for(Integer neighbor:neighbors){
				String value_to_emit_str = neighbor.toString() + " " + temp_value_to_emit_str;
				Text value_to_emit = new Text(value_to_emit_str);
				Text key_to_emit = new Text(getBlockId(neighbor).toString());
				context.write(key_to_emit, value_to_emit);
			}
		}
		
		// Re-emit the input key, value!
		context.write(new Text(block_id.toString()), value);
	}
	
	public Integer getBlockId(int node_id){
		int approx_block = (int) Math.floor(node_id/10000);
		
		if(approx_block == 0) 
			return 0;
		else if(approx_block == 68)
			return 67;
		
		while(approx_block > 0 && PageRank.cumulative_block_list[approx_block] > node_id)
			approx_block--;
		
		while(approx_block < 67 && PageRank.cumulative_block_list[approx_block] <= node_id)
			approx_block++;
				
		return approx_block;
	}
}
