import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewValidationMapper extends Mapper<LongWritable, Text, Text, Text> {

	List<String> stopwords = new ArrayList<>();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		// Access stopwords
		BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"));
		String line = null;

		while (true) {
			line = br.readLine();

			if (line != null) {
				stopwords.add(line);
			} else {
				break; // finished reading
			}
		}

		br.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] parts = value.toString().split("\t");

		String assembledValue = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
				parts[2], parts[3], parts[4], parts[5], parts[6], parts[7], parts[8], parts[9], parts[10], parts[11],
				parts[12], parts[13], removeStops(parts[17]), removeStops(parts[18]), removeStops(parts[19]),
				removeStops(parts[20]));

		context.write(new Text(parts[1]), new Text(assembledValue));

	}

	private String removeStops(String sentence) {
		
		String[] words = sentence.toLowerCase().replaceAll("[!\\\"#$%&'()*+,-./:;<=>?@\\\\[\\\\]^_`{|}~…’]", "").split(" ");
        StringBuilder sb = new StringBuilder();

        for (String word : words) {
            if (!stopwords.contains(word)) {
                sb.append(word).append(" ");
            }
        }

        String finalString = sb.toString().trim();
        
        return finalString.length() > 0 ? finalString : "null";
	}
}
