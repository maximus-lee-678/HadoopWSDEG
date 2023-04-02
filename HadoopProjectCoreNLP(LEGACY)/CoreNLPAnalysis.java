import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.AnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class CoreNLPAnalysis {
	private static CoreNLPAnalysis instance = null;
    private static Properties properties;
    private static String propertiesParams = "tokenize, ssplit, parse, sentiment";
    private static StanfordCoreNLP stanfordCoreNLP;

    private CoreNLPAnalysis() {
        properties = new Properties();
        properties.setProperty("annotators", propertiesParams);

        stanfordCoreNLP = new StanfordCoreNLP(properties);
    }

    public static CoreNLPAnalysis getInstance() {
        if (instance == null) {
            instance = new CoreNLPAnalysis();
        }
        return instance;
    }

    public Double getSentiment(String sentences) {
        Annotation annotation = stanfordCoreNLP.process(sentences);

        Integer sentenceCount = 0, totalSentiment = 0;

        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            // traversing the words in the current sentence
            Tree tree = sentence.get(AnnotatedTree.class);
            Integer sentimentInt = RNNCoreAnnotations.getPredictedClass(tree);
            // String sentimentName =
            // sentence.get(SentimentCoreAnnotations.ClassName.class);
            sentenceCount++;
            totalSentiment += sentimentInt;
        }
        
        return (double)totalSentiment / sentenceCount;
    }
}

