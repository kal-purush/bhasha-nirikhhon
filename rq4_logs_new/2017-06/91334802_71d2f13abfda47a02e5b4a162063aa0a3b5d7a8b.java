package emotionmining.perceptron;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import emotionmining.model.Tweet;


public class PerceprtronTest {

	public Map<String, Map<String, Double>> initializeWeightVecor() {

		Map<String, Map<String, Double>> weightVector = new HashMap<String, Map<String, Double>>();
		List<String> labels = Arrays.asList("anger", "disgust", "fear", "happy", "love", "sad", "surprise", "trust");
		// Initialize weight vectors for each class with bias 0.1
		for (String label : labels) {
			Map<String, Double> classWeightVector = new HashMap<String, Double>();
			classWeightVector.put("bias", 0.1);
			weightVector.put(label, classWeightVector);
		}
		return weightVector;
	}

	public Map<String, Map<String, Double>> trainModel(List<Tweet> tweetsList, int MAX_ITER) {

		// Evaluator eval = new Evaluator();
		Map<String, Double> tempWeights;
		List<Tweet> tweetsListEval;

		System.out.println("********** Train The Model ***********");

		Map<String, Map<String, Double>> weightVector = initializeWeightVecor();

		// ADD: MAX_ITER oder bis alle Instanzen richtig klassifiziert sind
		// Number of epochs
		for (int i = 0; i < MAX_ITER; i++) {

			tweetsListEval = new ArrayList<Tweet>();

			// for each point = for each tweet instance or each line in the
			// training data
			for (Tweet tweetInstance : tweetsList) {
				// Get the features for each instance (in form <FeatureName :
				// FeatureValue>)
				Map<String, Double> featureVector = tweetInstance.getFeatures();
				// Get the weights for each instance (in form <Label:
				// <FeatureName : FeatureValue>>)
				weightVector = getWeightVecors(featureVector.keySet(), weightVector);
				featureVector = putBiasInFeatureVector(featureVector);
				tweetInstance.setFeatures(featureVector);
				tweetInstance = getWinningPerceptron(tweetInstance, weightVector);

				if (tweetInstance.getMaxScoreCategory() == 0.0) {
					tempWeights = getAdjustedWeight(weightVector.get(tweetInstance.getGoldLabel()), true,
							featureVector);
					weightVector.put(tweetInstance.getGoldLabel(), tempWeights);
					continue;
				}
				if (!(tweetInstance.getGoldLabel().equalsIgnoreCase(tweetInstance.getPredictedLabel()))) {
					tempWeights = getAdjustedWeight(weightVector.get(tweetInstance.getGoldLabel()), true,
							featureVector);
					weightVector.put(tweetInstance.getGoldLabel(), tempWeights);
					tempWeights = getAdjustedWeight(weightVector.get(tweetInstance.getPredictedLabel()), false,
							featureVector);
					weightVector.put(tweetInstance.getPredictedLabel(), tempWeights);

				}
				tweetsListEval.add(tweetInstance);

			}
			System.out.println("Number of epochs is: " + i);
		}
		return weightVector;
	}

	public Map<String, Map<String, Double>> getWeightVecors(Set<String> features,
			Map<String, Map<String, Double>> weightMap) {

		// anger, disgust, fear, happy, love, sad, surprise, trust
		List<String> classLabels = Arrays.asList("anger", "disgust", "fear", "happy", "love", "sad", "surprise",
				"trust");

		for (String label : classLabels) {
			Map<String, Double> classWeightVector = weightMap.get(label);
			for (String featureStr : features) {
				if (classWeightVector.containsKey(featureStr)) {
					continue;
				} else {
					classWeightVector.put(featureStr, 0.1);
				}

			}
		}

		return weightMap;

	}

	// überarbeite. Füge lerning rate
	public Map<String, Double> putBiasInFeatureVector(Map<String, Double> featureVector) {
		featureVector.put("bias", 1.0);
		return featureVector;

	}

	public Tweet getWinningPerceptron(Tweet tweetInstance, Map<String, Map<String, Double>> weightMap) {

		double argmax = 0.0;
		String weighStr = "";
		Map<String, Double> featureVector = tweetInstance.getFeatures();
		for (String label : weightMap.keySet()) {
			// System.out.println("Label ::: "+label);
			double sum = sumFunktion(weightMap.get(label), featureVector);
			weighStr = weighStr + label + ":" + sum + ",";
			if (sum > argmax) {
				argmax = sum;
				tweetInstance.setPredictedLabel(label);
				tweetInstance.setMaxScoreCategory(sum);
			}
		}

		weighStr = weighStr.substring(0, weighStr.length() - 1);
		tweetInstance.setWeightStr(weighStr);
		return tweetInstance;

	}

	public Double sumFunktion(Map<String, Double> weights, Map<String, Double> featureVector) {

		double sum = 0.0;
		for (String feature : featureVector.keySet()) {
			if (weights.containsKey(feature)) {
				double w = weights.get(feature);
				double x = featureVector.get(feature);
				sum = sum + (w * x);
			}

		}

		return sum;

	}

	public Map<String, Double> getAdjustedWeight(Map<String, Double> weight, boolean action,
			Map<String, Double> featureVector) {

		// Map<String, Double> adjWeightVector = new HashMap<String, Double>();
		for (String feature : featureVector.keySet()) {

			double w = weight.get(feature);
			double x = featureVector.get(feature);
			if (action) {
				w = w + x;
			} else {
				w = w - x;
			}
			weight.put(feature, w);

		}

		return weight;

	}
}