import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Do not change the signature of this class
public class TextAnalyzer extends Configured implements Tool {

    // Replace "?" with your own output key / value types
    // The four template data types are:
    //     <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    public static class TextMapper extends Mapper<LongWritable, Text, Text, Tuple> {
    	private Text word = new Text();
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
        	HashSet<String> dups = new HashSet<String>();
        	String lowerCase = value.toString().toLowerCase().trim();       //make lowercase for values
        	lowerCase = lowerCase.replaceAll("[^A-Za-z0-9]", " ");
            StringTokenizer itr = new StringTokenizer(lowerCase);
            String [] words = lowerCase.split("\\s+");
            int index = 0;
	        while(itr.hasMoreTokens()){
	        	HashMap<String, Integer> list = new HashMap<String,Integer>();
	        	String token = itr.nextToken();
	        	if(dups.contains(token))
	        		continue;
	        	else
	        		dups.add(token);
	        	
	        	for(int i=0;i < words.length; i++ ){
	        		if(index == i)
	        			continue;
	        		if(list.containsKey(words[i]))
	        			list.put(words[i], list.get(words[i]) + 1);
	        		else
	        			list.put(words[i], 1);
	        	}
        		index++;
	    		String query = "";
	    		String num = "";
	    		
	    		for (Map.Entry<String, Integer> entry : list.entrySet()) {
	    			query = query + entry.getKey() + " ";
	    			num = num + entry.getValue()+ " ";
	    		}
	    		if(query.endsWith(" ")){
	    			query = query.substring(0, query.length() - 1);
	    		}
	    		
	    		if(num.endsWith(" ")){
	    			num = num.substring(0, num.length()-1);
	    		}
	    		
	    		Tuple tuple = new Tuple(new Text(query), new Text(num));
	    
	    		context.write(new Text(token),tuple);
	        	}
        }
    }

    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
    /*
    public static class TextCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Tuple> values, Context context)
            throws IOException, InterruptedException
        {
        	int count =0;
        	String paragraph = "";
            	for(Text queryWord: values){
            		count++;
            		paragraph = paragraph + " " + queryWord;
                }
            paragraph = paragraph.substring(1);
            String keyNum = key.toString() + " " + count;
            context.write(new Text(keyNum), new Text(paragraph));
        }
    }
  */

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    public static class TextReducer extends Reducer<Text, Tuple, Text, Text> {
        // key is context word, queryTuples is a dictionary where the key is the query word
        // and the value is the occurence of the word
        public void reduce(Text key, Iterable<Tuple> value, Context context)
            throws IOException, InterruptedException
        {
        	Text emptyText = new Text("");
        	Text queryWordText = new Text();
       // 	int count = 0;
        	HashMap<String,Integer> result = new HashMap<String, Integer>();
        	for(Tuple entry: value){
        //		count++;
        		String [] queries = entry.queryWord.toString().split("\\s+");
        		String [] queryCounts = entry.queryCount.toString().split("\\s+");
        		
        		for(int i =0; i < queries.length; i++){
        			String query = queries[i];
        			String queryCount = queryCounts[i];
        			if(query.equals(""))
        				continue;
        			else if(result.containsKey(query))
        				result.put(query, result.get(query)+Integer.parseInt(queryCount));
        			else
        				result.put(query, Integer.parseInt(queryCount));
        		}
        		
        	}
        	int max = 0;
        	String maxWord = "";
        	
        	for (Map.Entry<String, Integer> entry : result.entrySet()) {
        		if(entry.getValue() > max){
        			max = entry.getValue();
        			maxWord = entry.getKey();
        			
        		}
        	}
        	
        	context.write(key,new Text(Integer.toString(max)));
        	if(max!=0){
        		context.write(new Text("<" + maxWord + ","), new Text(max + ">"));
        		result.remove(maxWord);
        	}
        	
        	
        	ArrayList<String> sortedQueries = new ArrayList<String>();
        	for(String queryWord: result.keySet()){
        		sortedQueries.add(queryWord);
        	}
        	Collections.sort(sortedQueries, String.CASE_INSENSITIVE_ORDER);

            // Write out the results; you may change the following example
            // code to fit with your reducer function.
            //   Write out the current context key
            //   Write out query words and their count
            for(String queryWord: sortedQueries){
            	queryWord = queryWord.trim();
            	if(queryWord.equals(key.toString()))
            		continue;
                String num = result.get(queryWord) + ">";
                queryWordText.set("<" + queryWord + ",");
                context.write(queryWordText, new Text(num.trim()));
            }
            //   Empty line for ending the current context key
            context.write(emptyText, emptyText);
        }
       
        

    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "EID1_EID2"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);
        //   Uncomment the following line if you want to use Combiner class
  //      job.setCombinerClass(TextCombiner.class);
        job.setReducerClass(TextReducer.class);

        // Specify key / value types (Don't change them for the purpose of this assignment)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   If your mapper and combiner's  output types are different from Text.class,
        //   then uncomment the following lines to specify the data types.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Tuple.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // Do not modify the main method
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }

    // You may define sub-classes here. Example:
    public static class Tuple implements WritableComparable<Tuple> {
        private Text queryWord;
        private Text queryCount;

        public Tuple() {
            this.queryWord = new Text();
            this.queryCount = new Text();
        }

        public Tuple(Text word, Text count) {
            this.queryWord = word;
            this.queryCount = count;
        }

        public Tuple(String word, String count) {
            this.queryWord = new Text(word);
            this.queryCount = new Text(count);
        }

        public Text getQueryWord() {
            return queryWord;
        }

        public Text getQueryCount() {
            return queryCount;
        }

       /* public void increment(int additional) {
            queryCount.set(queryCount + additional);
        }*/

        
        public void readFields(DataInput in) throws IOException {
            queryWord.readFields(in);
            queryCount.readFields(in);
        }

        
        public void write(DataOutput out) throws IOException {
            queryWord.write(out);
            queryCount.write(out);
        }

        
        public int compareTo(Tuple other) {
            return queryWord.compareTo(other.queryWord);
        }

        @Override
        public int hashCode() {
            return queryWord.hashCode() * 163 + queryCount.hashCode();
        }
    }
}


