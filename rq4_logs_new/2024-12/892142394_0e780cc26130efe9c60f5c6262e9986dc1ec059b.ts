import { Result } from '@/types/result';

export function processResults(process: Result['results']) {
  const leafClassNames = ['Cercospora', 'Coffee Rust', 'Phoma'];
  const leafClassIndex = [0, 1, 4];

  const severityClassNames = ['Low', 'Moderate', 'Severe', 'Mild'];
  const severityClassIndex = [2, 3, 5, 6];

  // First, let's fix the confidence mapping in transpose
  const transpose = process.map(response => {
    const classIndex = response.class;
    return {
      bbox: response.bbox,
      class: leafClassIndex.includes(classIndex)
        ? leafClassIndex.indexOf(classIndex)
        : severityClassIndex.indexOf(classIndex),
      confidence: response.confidence, // Fixed: was using bbox instead of confidence
    };
  });

  // Filter detections by leaf classes
  const leafDetections = transpose.filter(item =>
    leafClassIndex.includes(process[transpose.indexOf(item)].class),
  );

  // Filter detections by severity classes
  const severityDetections = transpose.filter(item =>
    severityClassIndex.includes(process[transpose.indexOf(item)].class),
  );

  // Calculate average confidence for leaf detections and get the most likely class
  let dominantLeafDisease = null;
  if (leafDetections.length > 0) {
    const leafConfidenceMap = new Map<number, { sum: number; count: number }>();

    leafDetections.forEach(detection => {
      const currentData = leafConfidenceMap.get(detection.class) || {
        sum: 0,
        count: 0,
      };
      leafConfidenceMap.set(detection.class, {
        sum: currentData.sum + detection.confidence,
        count: currentData.count + 1,
      });
    });

    let highestAvgConfidence = 0;
    let mostLikelyClass = 0;

    leafConfidenceMap.forEach((value, classIndex) => {
      const avgConfidence = value.sum / value.count;
      if (avgConfidence > highestAvgConfidence) {
        highestAvgConfidence = avgConfidence;
        mostLikelyClass = classIndex;
      }
    });

    dominantLeafDisease = {
      class: leafClassNames[mostLikelyClass],
      confidence: highestAvgConfidence,
    };
  }

  // Map severity detections to their class names
  let dominantSeverity = null;
  if (severityDetections.length > 0) {
    const highestConfidenceSeverity = severityDetections.reduce(
      (prev, current) =>
        prev.confidence > current.confidence ? prev : current,
    );

    dominantSeverity = {
      className: severityClassNames[highestConfidenceSeverity.class],
      confidence: highestConfidenceSeverity.confidence,
    };
  }

  return {
    disease: dominantLeafDisease!,
    severities: dominantSeverity!,
  };
}