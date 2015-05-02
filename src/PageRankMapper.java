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
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		Integer node_id = Integer.parseInt(tokenizer.nextToken());
		Double page_rank = Double.parseDouble(tokenizer.nextToken());
		Integer degree = Integer.parseInt(tokenizer.nextToken());
		
		Vector<Integer> neighbors = new Vector<Integer>();
		
		for(int i=0; i<degree; i++){
			neighbors.add(Integer.parseInt(tokenizer.nextToken()));
		}
		
		if(degree > 0){
			Double double_value_to_emit = page_rank/degree;
			Text value_to_emit = new Text(double_value_to_emit.toString());
		
			for(Integer neighbor:neighbors){
				Text key_to_emit = new Text(neighbor.toString());
				context.write(key_to_emit, value_to_emit);
			}
		}
		
		context.write(new Text(node_id.toString()), value);
	}
}
