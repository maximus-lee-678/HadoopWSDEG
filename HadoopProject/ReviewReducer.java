
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReviewReducer extends Reducer<Text, Text, Text, Text> {
	List<FinalReviewConstruction> allFinalReviews = new ArrayList<FinalReviewConstruction>();


	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		FinalReviewConstruction finalReviewConstruction = new FinalReviewConstruction();
		
		finalReviewConstruction.setCompany(key.toString());
		
		System.out.println("Reducing: " + key.toString());
		
		for (Text t : values) {
			finalReviewConstruction.incrementReviewCount();
			
			String parts[] = t.toString().split("\t");
			
			finalReviewConstruction.incrementMap("rating", Integer.parseInt(parts[0]));
			finalReviewConstruction.incrementMap("recommendationRating", Integer.parseInt(parts[1]));
			finalReviewConstruction.incrementMap("CEOApprovalRating", Integer.parseInt(parts[2]));
			finalReviewConstruction.incrementMap("businessOutlookRating", Integer.parseInt(parts[3]));
			finalReviewConstruction.incrementMap("workLifeBalanceRating", Integer.parseInt(parts[4]));
			finalReviewConstruction.incrementMap("cultureValuesRating", Integer.parseInt(parts[5]));
			finalReviewConstruction.incrementMap("diversityInclusionRating", Integer.parseInt(parts[6]));
			finalReviewConstruction.incrementMap("careerOpportunitiesRating", Integer.parseInt(parts[7]));
			finalReviewConstruction.incrementMap("compensationBenefitsRating", Integer.parseInt(parts[8]));
			finalReviewConstruction.incrementMap("seniorManagementRating", Integer.parseInt(parts[9]));
			finalReviewConstruction.tryIncrementCurrentJobCount(Integer.parseInt(parts[10]));
			finalReviewConstruction.incrementMap("employmentLength", Integer.parseInt(parts[11]));
			finalReviewConstruction.incrementMap("reviewSummarySentiment", Double.parseDouble(parts[12]));
			finalReviewConstruction.incrementMap("reviewProsSentiment", Double.parseDouble(parts[13]));
			finalReviewConstruction.incrementMap("reviewConsSentiment", Double.parseDouble(parts[14]));
			finalReviewConstruction.incrementMap("reviewAdviceSentiment", Double.parseDouble(parts[15]));
		}
		allFinalReviews.add(finalReviewConstruction);
		
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for(FinalReviewConstruction finalReview : allFinalReviews) {
			context.write(new Text(finalReview.getCompany()), 
					new Text(String.format("%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"
							+ "\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
							finalReview.getReviewCount(),
							finalReview.getMapAverage("rating"), finalReview.getMapMedian("rating"),
							finalReview.getMapAverage("recommendationRating"), finalReview.getMapMedian("recommendationRating"), finalReview.getPercent("recommendationRating"),
							finalReview.getMapAverage("CEOApprovalRating"), finalReview.getMapMedian("CEOApprovalRating"), finalReview.getPercent("CEOApprovalRating"),
							finalReview.getMapAverage("businessOutlookRating"), finalReview.getMapMedian("businessOutlookRating"), finalReview.getPercent("businessOutlookRating"),
							finalReview.getMapAverage("workLifeBalanceRating"), finalReview.getMapMedian("workLifeBalanceRating"), finalReview.getPercent("workLifeBalanceRating"),
							finalReview.getMapAverage("cultureValuesRating"), finalReview.getMapMedian("cultureValuesRating"), finalReview.getPercent("cultureValuesRating"),
							finalReview.getMapAverage("diversityInclusionRating"), finalReview.getMapMedian("diversityInclusionRating"), finalReview.getPercent("diversityInclusionRating"),
							finalReview.getMapAverage("careerOpportunitiesRating"), finalReview.getMapMedian("careerOpportunitiesRating"), finalReview.getPercent("careerOpportunitiesRating"),
							finalReview.getMapAverage("compensationBenefitsRating"), finalReview.getMapMedian("compensationBenefitsRating"), finalReview.getPercent("compensationBenefitsRating"),
							finalReview.getMapAverage("seniorManagementRating"), finalReview.getMapMedian("seniorManagementRating"), finalReview.getPercent("seniorManagementRating"),
							finalReview.getPercent("isCurrentJob"),
							finalReview.getMapAverage("employmentLength"), finalReview.getMapMedian("employmentLength"),
							finalReview.getMapAverage("reviewSummarySentiment"), finalReview.getMapMedian("reviewSummarySentiment"),
							finalReview.getMapAverage("reviewProsSentiment"), finalReview.getMapMedian("reviewProsSentiment"),
							finalReview.getMapAverage("reviewConsSentiment"), finalReview.getMapMedian("reviewConsSentiment"),
							finalReview.getMapAverage("reviewAdviceSentiment"), finalReview.getMapMedian("reviewAdviceSentiment")
							)));
		}
	}
}