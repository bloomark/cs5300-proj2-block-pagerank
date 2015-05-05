import java.io.IOException;
import java.util.HashMap;
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
		Vector<Integer> nodes_in_block = new Vector<Integer>();
		HashMap<Integer, Double> page_ranks = new HashMap<Integer, Double>();
		HashMap<Integer, Double> page_ranks_copy = new HashMap<Integer, Double>();
		HashMap<Integer, Vector<Integer> > incoming_edges = new HashMap<Integer, Vector<Integer> >();
		HashMap<Integer, Double> BC = new HashMap<Integer, Double>();
		HashMap<Integer, String> adjecency_list = new HashMap<Integer, String>();
		HashMap<Integer, Integer> degrees = new HashMap<Integer, Integer>();
		
		Integer block_id = Integer.parseInt(key.toString());
		Double page_rank = 0.0;
		Integer degree = 0;
		String neighbors = "";
		
		while(iter.hasNext()){
			Text input = iter.next();
			
			String line = input.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			//System.out.println("Line = " + line);
			
			if(tokenizer.countTokens() == 3){
				//tokenizer = new StringTokenizer(line);
				// Input was <block_id(v)> <v block_id(u) page_rank/degree>
				Integer node_id = Integer.parseInt(tokenizer.nextToken());
				Integer incoming_block_id = Integer.parseInt(tokenizer.nextToken());
				Double incoming_PR = Double.parseDouble(tokenizer.nextToken());
				if(!BC.containsKey(node_id))
					BC.put(node_id, 0.0);
				if(block_id != incoming_block_id)
					BC.put(node_id, BC.get(node_id) + incoming_PR);
			}
			else{
				//tokenizer = new StringTokenizer(line);
				// Input was <block_id(v)> <block_id(v)> v page_rank> <degree> [<neighbour1> <neighbor2>....]
				tokenizer.nextToken();
				Integer node_id = Integer.parseInt(tokenizer.nextToken());
				page_rank = Double.parseDouble(tokenizer.nextToken());
				degree = Integer.parseInt(tokenizer.nextToken());
				neighbors = "";
				degrees.put(node_id, degree);
				page_ranks.put(node_id, page_rank);
				page_ranks_copy.put(node_id, page_rank);
				nodes_in_block.addElement(node_id);
				
				for(int i=0; i<degree; i++){
					Integer neighbour_node = Integer.parseInt(tokenizer.nextToken());
					if(!incoming_edges.containsKey(neighbour_node))
						incoming_edges.put(neighbour_node, new Vector<Integer>());
					if(getBlockId(node_id) == block_id)
						incoming_edges.get(neighbour_node).addElement(node_id);
					
					neighbors = neighbors + neighbour_node.toString() + " ";
				}
				neighbors = neighbors.trim();
				adjecency_list.put(node_id, neighbors);
			}
		}
		
		
		/*
		 * We have now generated page_ranks, incoming_edges, BC, adjecency_list, degrees
		 */
		int num_runs = 0; 
		Double residual_error = Double.MAX_VALUE;
		HashMap<Integer, Double> new_page_ranks = new HashMap<Integer, Double>();
		while(num_runs < 5 && residual_error > 0.001)
		{
			/*
			 * Calculate new page ranks
			 */
			residual_error = 0.0;
			for(int i=0; i<nodes_in_block.size(); ++i)
			{
				Integer cur_node_id = nodes_in_block.elementAt(i);
				Double new_page_rank = 0.0;
				if(BC.containsKey(cur_node_id))
					new_page_rank += BC.get(cur_node_id);
				Vector<Integer> incoming_edge_list = incoming_edges.get(cur_node_id);
				if(incoming_edge_list != null)
				{
					for(int j=0; j<incoming_edge_list.size(); ++j)
					{
						Integer edge = incoming_edge_list.elementAt(j);
						new_page_rank += page_ranks.get(edge)/degrees.get(edge);
					}
				}
				new_page_rank = CONSTANT_VALUE + DAMPING_FACTOR * new_page_rank;
				new_page_ranks.put(cur_node_id, new_page_rank);
				
				//Calculate Residual
				residual_error += Math.abs((new_page_rank - page_ranks.get(cur_node_id))/new_page_rank);
			}
			for(int i=0; i<nodes_in_block.size(); i++)
			{
				Integer node_id = nodes_in_block.elementAt(i);
				Double new_page_rank = new_page_ranks.get(node_id);
				page_ranks.put(node_id, new_page_rank);
			}
			//page_ranks = new_page_ranks;
			num_runs++;
			residual_error = residual_error/nodes_in_block.size();
		}
		
		Double residual_error_for_run = 0.0;
		for(int i=0; i<nodes_in_block.size(); ++i)
		{
			Double page_rank_val      = page_ranks.get(nodes_in_block.elementAt(i));
			Double page_rank_val_old  = page_ranks_copy.get(nodes_in_block.elementAt(i));
			residual_error_for_run   += Math.abs((page_rank_val-page_rank_val_old)/page_rank_val);
		}
		
		Long long_residual_error = (long) (residual_error_for_run * 10e5);
		context.getCounter(PageRankCounter.RESIDUAL).increment(long_residual_error);
		context.getCounter(PageRankCounter.INCREMENTS).increment(num_runs);
		
		for(int i=0; i<nodes_in_block.size(); ++i)
		{
			Integer cur_node_id = nodes_in_block.elementAt(i);
			Text key_to_emit = new Text(block_id.toString() + " " + cur_node_id.toString());
			Text value_to_emit = new Text( page_ranks.get(cur_node_id).toString() + " " + degrees.get(cur_node_id).toString() + " " + adjecency_list.get(cur_node_id));
			context.write(key_to_emit, value_to_emit);
		}
	}
	
	public static boolean isBlockID(StringTokenizer tokenizer){
		tokenizer.nextToken();
		if(tokenizer.nextToken().contains("."))
			return false;
		return true;
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
