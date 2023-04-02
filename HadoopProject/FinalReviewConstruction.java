
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

public class FinalReviewConstruction {
	private String company;
	private Integer reviewCount;

	private Map<Integer, Integer> ratingMap; // 1 - 5
	private Map<Integer, Integer> recommendationRatingMap; // 1 - 2
	private Map<Integer, Integer> CEOApprovalRatingMap; // 1 - 3
	private Map<Integer, Integer> businessOutlookRatingMap; // 1 - 3
	private Map<Integer, Integer> workLifeBalanceRatingMap; // 1 - 5
	private Map<Integer, Integer> cultureValuesRatingMap; // 1 - 5
	private Map<Integer, Integer> diversityInclusionRatingMap; // 1 - 5
	private Map<Integer, Integer> careerOpportunitiesRatingMap; // 1 - 5
	private Map<Integer, Integer> compensationBenefitsRatingMap; // 1 - 5
	private Map<Integer, Integer> seniorManagementRatingMap; // 1 - 5

	private Integer isCurrentJobCount;

	private Map<Integer, Integer> employmentLengthMap; // >=0

	private Map<Double, Integer> reviewSummarySentimentMap;
	private Map<Double, Integer> reviewProsSentimentMap;
	private Map<Double, Integer> reviewConsSentimentMap;
	private Map<Double, Integer> reviewAdviceSentimentMap;

	FinalReviewConstruction() {
		this.setCompany("");
		this.reviewCount = 0;

		this.ratingMap = initMap(5);
		this.recommendationRatingMap = initMap(2);
		this.CEOApprovalRatingMap = initMap(3);
		this.businessOutlookRatingMap = initMap(3);
		this.workLifeBalanceRatingMap = initMap(5);
		this.cultureValuesRatingMap = initMap(5);
		this.diversityInclusionRatingMap = initMap(5);
		this.careerOpportunitiesRatingMap = initMap(5);
		this.compensationBenefitsRatingMap = initMap(5);
		this.seniorManagementRatingMap = initMap(5);

		this.isCurrentJobCount = 0;

		this.employmentLengthMap = new TreeMap<Integer, Integer>();

		this.reviewSummarySentimentMap = new TreeMap<Double, Integer>();
		this.reviewProsSentimentMap = new TreeMap<Double, Integer>();
		this.reviewConsSentimentMap = new TreeMap<Double, Integer>();
		this.reviewAdviceSentimentMap = new TreeMap<Double, Integer>();
	}

	private Map<Integer, Integer> initMap(int maxValue) {
		Map<Integer, Integer> map = new LinkedHashMap<Integer, Integer>();
		for (int i = 1; i <= maxValue; i++) {
			map.put(i, 0);
		}

		return map;
	}

	public String getCompany() {
		return company;
	}

	public void setCompany(String company) {
		this.company = company;
	}

	public Integer getReviewCount() {
		return reviewCount;
	}

	public void incrementReviewCount() {
		this.reviewCount++;
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////

	public void incrementMap(String reviewType, Integer value) {
		if (value == 0 && !reviewType.equals("employmentLength"))
			return;

		switch (reviewType) {
		case "rating":
			ratingMap.put(value, ratingMap.get(value) + 1);
			break;
		case "recommendationRating":
			recommendationRatingMap.put(value, recommendationRatingMap.get(value) + 1);
			break;
		case "CEOApprovalRating":
			CEOApprovalRatingMap.put(value, CEOApprovalRatingMap.get(value) + 1);
			break;
		case "businessOutlookRating":
			businessOutlookRatingMap.put(value, businessOutlookRatingMap.get(value) + 1);
			break;
		case "workLifeBalanceRating":
			workLifeBalanceRatingMap.put(value, workLifeBalanceRatingMap.get(value) + 1);
			break;
		case "cultureValuesRating":
			cultureValuesRatingMap.put(value, cultureValuesRatingMap.get(value) + 1);
			break;
		case "diversityInclusionRating":
			diversityInclusionRatingMap.put(value, diversityInclusionRatingMap.get(value) + 1);
			break;
		case "careerOpportunitiesRating":
			careerOpportunitiesRatingMap.put(value, careerOpportunitiesRatingMap.get(value) + 1);
			break;
		case "compensationBenefitsRating":
			compensationBenefitsRatingMap.put(value, compensationBenefitsRatingMap.get(value) + 1);
			break;
		case "seniorManagementRating":
			seniorManagementRatingMap.put(value, seniorManagementRatingMap.get(value) + 1);
			break;
		case "employmentLength":
			if (employmentLengthMap.get(value) == null)
				employmentLengthMap.put(value, 1);
			else
				employmentLengthMap.put(value, employmentLengthMap.get(value) + 1);
			break;
		}
	}

	public void incrementMap(String reviewType, Double value) {
		switch (reviewType) {
		case "reviewSummarySentiment":
			if (reviewSummarySentimentMap.get(value) == null)
				reviewSummarySentimentMap.put(value, 1);
			else
				reviewSummarySentimentMap.put(value, reviewSummarySentimentMap.get(value) + 1);
			break;
		case "reviewProsSentiment":
			if (reviewProsSentimentMap.get(value) == null)
				reviewProsSentimentMap.put(value, 1);
			else
				reviewProsSentimentMap.put(value, reviewProsSentimentMap.get(value) + 1);
			break;
		case "reviewConsSentiment":
			if (reviewConsSentimentMap.get(value) == null)
				reviewConsSentimentMap.put(value, 1);
			else
				reviewConsSentimentMap.put(value, reviewConsSentimentMap.get(value) + 1);
			break;
		case "reviewAdviceSentiment":
			if(value == -6.0){
				break;
			}
			if (reviewAdviceSentimentMap.get(value) == null)
				reviewAdviceSentimentMap.put(value, 1);
			else
				reviewAdviceSentimentMap.put(value, reviewAdviceSentimentMap.get(value) + 1);
			break;
		}
	}

	public String getMapAverage(String reviewType) {
		switch (reviewType) {
		case "rating":
			return String.format("%.2f", computeAverage(ratingMap, reviewCount));

		case "recommendationRating":
			return String.format("%.2f",
					computeAverage(recommendationRatingMap, countValuesInMap(recommendationRatingMap)));

		case "CEOApprovalRating":
			return String.format("%.2f", computeAverage(CEOApprovalRatingMap, countValuesInMap(CEOApprovalRatingMap)));

		case "businessOutlookRating":
			return String.format("%.2f",
					computeAverage(businessOutlookRatingMap, countValuesInMap(businessOutlookRatingMap)));

		case "workLifeBalanceRating":
			return String.format("%.2f",
					computeAverage(workLifeBalanceRatingMap, countValuesInMap(workLifeBalanceRatingMap)));

		case "cultureValuesRating":
			return String.format("%.2f",
					computeAverage(cultureValuesRatingMap, countValuesInMap(cultureValuesRatingMap)));

		case "diversityInclusionRating":
			return String.format("%.2f",
					computeAverage(diversityInclusionRatingMap, countValuesInMap(diversityInclusionRatingMap)));

		case "careerOpportunitiesRating":
			return String.format("%.2f",
					computeAverage(careerOpportunitiesRatingMap, countValuesInMap(careerOpportunitiesRatingMap)));

		case "compensationBenefitsRating":
			return String.format("%.2f",
					computeAverage(compensationBenefitsRatingMap, countValuesInMap(compensationBenefitsRatingMap)));

		case "seniorManagementRating":
			return String.format("%.2f",
					computeAverage(seniorManagementRatingMap, countValuesInMap(seniorManagementRatingMap)));

		case "employmentLength":
			return String.format("%.2f", computeAverage(employmentLengthMap, reviewCount));

		case "reviewSummarySentiment":
			return String.format("%.2f", computeAverageD(reviewSummarySentimentMap, reviewCount));

		case "reviewProsSentiment":
			return String.format("%.2f", computeAverageD(reviewProsSentimentMap, reviewCount));

		case "reviewConsSentiment":
			return String.format("%.2f", computeAverageD(reviewConsSentimentMap, reviewCount));

		case "reviewAdviceSentiment":
			return String.format("%.2f",
					computeAverageD(reviewConsSentimentMap, (countValuesInMapD(reviewAdviceSentimentMap))));

		default:
			return "";
		}
	}

	private double computeAverage(Map<Integer, Integer> map, int count) {

		double total = 0.0;
		for (Entry<Integer, Integer> entry : map.entrySet()) {
			total += entry.getKey() * entry.getValue();
		}

		return total / count;
	}

	private double computeAverageD(Map<Double, Integer> map, int count) {

		double total = 0.0;
		for (Entry<Double, Integer> entry : map.entrySet()) {
			total += entry.getKey() * entry.getValue();
		}

		return total / count;
	}

	public String getMapMedian(String reviewType) {
		switch (reviewType) {
		case "rating":
			return String.format("%d", getValueAtIndex(ratingMap, calcMedian(reviewCount)));

		case "recommendationRating":
			return String.format("%d",
					getValueAtIndex(recommendationRatingMap, calcMedian(countValuesInMap(recommendationRatingMap))));

		case "CEOApprovalRating":
			return String.format("%d",
					getValueAtIndex(CEOApprovalRatingMap, calcMedian(countValuesInMap(CEOApprovalRatingMap))));

		case "businessOutlookRating":
			return String.format("%d",
					getValueAtIndex(businessOutlookRatingMap, calcMedian(countValuesInMap(businessOutlookRatingMap))));

		case "workLifeBalanceRating":
			return String.format("%d",
					getValueAtIndex(workLifeBalanceRatingMap, calcMedian(countValuesInMap(workLifeBalanceRatingMap))));

		case "cultureValuesRating":
			return String.format("%d",
					getValueAtIndex(cultureValuesRatingMap, calcMedian(countValuesInMap(cultureValuesRatingMap))));

		case "diversityInclusionRating":
			return String.format("%d", getValueAtIndex(diversityInclusionRatingMap,
					calcMedian(countValuesInMap(diversityInclusionRatingMap))));

		case "careerOpportunitiesRating":
			return String.format("%d", getValueAtIndex(careerOpportunitiesRatingMap,
					calcMedian(countValuesInMap(careerOpportunitiesRatingMap))));

		case "compensationBenefitsRating":
			return String.format("%d", getValueAtIndex(compensationBenefitsRatingMap,
					calcMedian(countValuesInMap(compensationBenefitsRatingMap))));

		case "seniorManagementRating":
			return String.format("%d", getValueAtIndex(seniorManagementRatingMap,
					calcMedian(countValuesInMap(seniorManagementRatingMap))));

		case "employmentLength":
			return String.format("%d", getValueAtIndex(employmentLengthMap, calcMedian(reviewCount)));

		case "reviewSummarySentiment":
			return String.format("%.2f", getValueAtIndexD(reviewSummarySentimentMap, calcMedian(reviewCount)));

		case "reviewProsSentiment":
			return String.format("%.2f", getValueAtIndexD(reviewProsSentimentMap, calcMedian(reviewCount)));

		case "reviewConsSentiment":
			return String.format("%.2f", getValueAtIndexD(reviewConsSentimentMap, calcMedian(reviewCount)));

		case "reviewAdviceSentiment":
			return String.format("%.2f", getValueAtIndexD(reviewAdviceSentimentMap,
					calcMedian(countValuesInMapD(reviewAdviceSentimentMap))));

		default:
			return "";
		}
	}

	private int countValuesInMap(Map<Integer, Integer> map) {
		int count = 0;

		for (Entry<Integer, Integer> entry : map.entrySet()) {
			count += entry.getValue();
		}

		return count;
	}

	// worst overloading ever
	private int countValuesInMapD(Map<Double, Integer> map) {
		int count = 0;

		for (Entry<Double, Integer> entry : map.entrySet()) {
			count += entry.getValue();
		}

		return count;
	}

	private Integer getValueAtIndex(Map<Integer, Integer> map, int index) {
		if (index == 0)
			return null;

		int currentIndex = 0;

		for (Entry<Integer, Integer> entry : map.entrySet()) {
			currentIndex += entry.getValue();

			if (currentIndex >= index)
				return entry.getKey();
		}

		return -1;
	}

	// worst overloading ever
	private Double getValueAtIndexD(Map<Double, Integer> map, int index) {
		if (index == 0)
			return null;

		int currentIndex = 0;

		for (Entry<Double, Integer> entry : map.entrySet()) {
			currentIndex += entry.getValue();

			if (currentIndex >= index)
				return entry.getKey();
		}

		return -1.0;
	}

	private int calcMedian(int value) {
		return value % 2 != 0 ? (value + 1) / 2 : ((value / 2) + (value / 2 + 1)) / 2;
	}

	public String getPercent(String reviewType) {
		switch (reviewType) {
		case "recommendationRating":
			return String.format("%.2f", (double) countValuesInMap(recommendationRatingMap) / reviewCount * 100);
		case "CEOApprovalRating":
			return String.format("%.2f", (double) countValuesInMap(CEOApprovalRatingMap) / reviewCount * 100);
		case "businessOutlookRating":
			return String.format("%.2f", (double) countValuesInMap(businessOutlookRatingMap) / reviewCount * 100);
		case "workLifeBalanceRating":
			return String.format("%.2f", (double) countValuesInMap(workLifeBalanceRatingMap) / reviewCount * 100);
		case "cultureValuesRating":
			return String.format("%.2f", (double) countValuesInMap(cultureValuesRatingMap) / reviewCount * 100);
		case "diversityInclusionRating":
			return String.format("%.2f", (double) countValuesInMap(diversityInclusionRatingMap) / reviewCount * 100);
		case "careerOpportunitiesRating":
			return String.format("%.2f", (double) countValuesInMap(careerOpportunitiesRatingMap) / reviewCount * 100);
		case "compensationBenefitsRating":
			return String.format("%.2f", (double) countValuesInMap(compensationBenefitsRatingMap) / reviewCount * 100);
		case "seniorManagementRating":
			return String.format("%.2f", (double) countValuesInMap(seniorManagementRatingMap) / reviewCount * 100);
		case "isCurrentJob":
			return String.format("%.2f", (double) isCurrentJobCount / reviewCount * 100);
		case "reviewAdviceSentiment":
			return String.format("%.2f", (double) countValuesInMapD(reviewAdviceSentimentMap) / reviewCount * 100);
		default:
			return String.format("%.2f", 0.00);
		}

	}

	////////////////////////////////////////////////////////////////////////////////////////////////////

	public void tryIncrementCurrentJobCount(Integer isCurrentJob) {
		if (isCurrentJob == 1)
			this.isCurrentJobCount++;
	}
}
