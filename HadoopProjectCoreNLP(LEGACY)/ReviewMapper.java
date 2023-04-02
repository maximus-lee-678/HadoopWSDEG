
import java.io.IOException;
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

public class ReviewMapper extends Mapper<LongWritable, Text, Text, Text> {
	CoreNLPAnalysis coreNLPAnalysis = CoreNLPAnalysis.getInstance();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException{
		String[] parts = value.toString().split("\t");
		
      ExecutorService executor = Executors.newFixedThreadPool(4);

      Callable<Double> task1 = new CoreNLPThread(parts[17]);
      Callable<Double> task2 = new CoreNLPThread(parts[18]);
      Callable<Double> task3 = new CoreNLPThread(parts[19]);
      Callable<Double> task4 = new CoreNLPThread(parts[20]);

      Future<Double> sentimentSummaryFuture = executor.submit(task1);
      Future<Double> sentimentProsFuture = executor.submit(task2);
      Future<Double> sentimentConsFuture = executor.submit(task3);
      Future<Double> sentimentAdviceFuture = executor.submit(task4);

      Double sentimentSummary = 0.0;
      Double sentimentPros = 0.0;
      Double sentimentCons = 0.0;
      Double sentimentAdvice = 0.0;
      
      try{
      	sentimentSummary = sentimentSummaryFuture.get();
//          System.out.println(sentimentSummary);
          sentimentPros = sentimentProsFuture.get();
//          System.out.println(sentimentPros);
          sentimentCons = sentimentConsFuture.get();
//          System.out.println(sentimentCons);
          sentimentAdvice = sentimentAdviceFuture.get();
//          System.out.println(sentimentAdvice);
      }catch(ExecutionException e){
      	e.printStackTrace();
      }

//		Double sentimentSummary = 1.0;
//		Double sentimentPros = 2.0;
//		Double sentimentCons = 3.0;
//		Double sentimentAdvice = 4.0;


		String assembledValue = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t",
				parts[2], parts[3], parts[4], parts[5], parts[6], parts[7], parts[8], parts[9], parts[10], parts[11], parts[12], parts[13],
				sentimentSummary.toString(), sentimentPros.toString(), sentimentCons.toString(), sentimentAdvice.toString());

		context.write(new Text(parts[1].trim()), new Text(assembledValue));
		
		executor.shutdown();
//		task1 = null;
//		task2 = null;
//		task3 = null;
//		task4 = null;
//		sentimentSummaryFuture = null;
//		sentimentProsFuture = null;
//		sentimentConsFuture = null;
//		sentimentAdviceFuture = null;
//		System.gc();
	}
	
  private class CoreNLPThread implements Callable<Double> {
  	private String sentence;
  	
  	public CoreNLPThread(String sentence){
  		this.sentence = sentence;
  	}
  	
      @Override
      public Double call() throws Exception {
//      	System.out.println(sentence);
      	Double sentiment = coreNLPAnalysis.getSentiment(sentence);
      	System.out.println(sentiment);
      		return sentiment;
      }
  }
}

