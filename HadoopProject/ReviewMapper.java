
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper Output:
//Key:
//[0] Company Name
//Values(\t separated):
//[1] Overall Rating			(1-5) average
//[2] Recommend? 				(1-2) <no, yes>	average
//[3] CEO Approval?			(1-3) nullable	<no, no input, yes>
//[4] Business Outlook?			(1-3) nullable	<no, no input, yes>
//[5] Work/Life Balance			(1-5) nullable*	average w/percent
//[6] Culture & Values			(1-5) nullable*	average w/percent
//[7] Diversity and Inclusion		(1-5) nullable*	average w/percent
//[8] Career Opportunities		(1-5) nullable*	average w/percent
//[9] Compensation and Benefits		(1-5) nullable*	average w/percent
//[10] Senior Management			(1-5) nullable*	average w/percent
//[11] Is current job?			(0-1)	average
//[12] Length of Employment		(>=0)**	average
//[16] Review Summary			string	average sentiment
//[17] Pros				string	average sentiment
//[18] Cons				string	average sentiment
//[19] Advice to management		string nullable	average sentiment

public class ReviewMapper extends Mapper<Text, Text, Text, Text> {

	Hashtable<String, Integer> sentiments = new Hashtable<>();

	@Override
	protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

		// Access sentiment
		BufferedReader br = new BufferedReader(new FileReader("AFINN-111.txt"));
		String line = null;

		while (true) {
			line = br.readLine();

			if (line != null) {
				String parts[] = line.split("\t");
				sentiments.put(parts[0], Integer.parseInt(parts[1]));
			} else {
				break; // finished reading
			}
		}

		br.close();
	}

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] parts = value.toString().split("\t");

			String assembledValue = String.format(
					"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%.2f\t%.2f\t%.2f\t%.2f", parts[0], parts[1],
					parts[2], parts[3], parts[4], parts[5], parts[6], parts[7], parts[8], parts[9], parts[10],
					parts[11], getSentiment(parts[12]), getSentiment(parts[13]), getSentiment(parts[14]),
					getSentiment(parts[15]));
			context.write(new Text(key.toString()), new Text(assembledValue));
	}

	private double getSentiment(String sentence) {
		String parts[] = sentence.split(" ");
		Integer sentimentValue = 0, wordCount = 0;
		
		if(sentence.equals("null"))
			return -6.0;

		for (String part : parts) {
			if (part.isEmpty() || part == "") {
				continue;
			}
			
			wordCount++;
			Integer singleSentiment = sentiments.get(part);
			if (singleSentiment != null) {
				sentimentValue += singleSentiment;
			}
		}

		return (double) sentimentValue / wordCount;
	}

}
