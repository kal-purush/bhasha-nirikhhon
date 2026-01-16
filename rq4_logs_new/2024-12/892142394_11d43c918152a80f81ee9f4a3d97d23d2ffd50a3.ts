type BBox = [number, number, number, number];
interface Detections {
  bbox: BBox;
  confidence: any;
  classIndex: number;
}

export const processYoloOutput = (
  modelOutput: any,
  confidenceThreshold = 0.25,
  iouThreshold = 0.45,
) => {
  // Reshape the output from [1, 11, 8400] to [8400, 11]
  console.log('starting processing');
  const detections = [];
  const output = modelOutput[0]; // Get first batch

  // Reshape and transpose the data
  console.log('reshaping data');
  for (let i = 0; i < 8400; i++) {
    const detection = [];
    for (let j = 0; j < 11; j++) {
      detection.push(output[j][i]);
    }
    detections.push(detection);
  }

  // Filter by confidence
  console.log('filtering data');
  const validDetections = detections
    .map(detection => {
      // Assuming format: [x, y, w, h, confidence, class_scores...]
      const confidence = detection[4];
      if (confidence < confidenceThreshold) {
        return null;
      }

      // Get class with highest score
      const classScores = detection.slice(5);
      const classIndex = classScores.indexOf(Math.max(...classScores));

      return {
        bbox: detection.slice(0, 4),
        confidence: confidence,
        classIndex: classIndex,
      };
    })
    .filter(Boolean); // Remove null values

  // Perform NMS
  console.log('calculating threshold');
  const finalDetections = nms(validDetections, iouThreshold);

  // Format to desired output
  const result = finalDetections.map(det => {
    return {
      classId: det.classIndex,
      bbox: [det.bbox[0], det.bbox[1], det.bbox[2], det.bbox[3]],
      confidence: det.confidence,
    };
    // `${det.classIndex} ${det.bbox[0]} ${det.bbox[1]} ${det.bbox[2]} ${det.bbox[3]} ${det.confidence}`;
  });

  console.log(result);

  return result;
};

// Non-max suppression implementation
const nms = (boxes: any, iouThreshold: number) => {
  // Sort by confidence
  console.log('sorting boxes');
  boxes.sort((a: Detections, b: Detections) => b.confidence - a.confidence);

  const selected = [];
  const suppress = new Set();

  console.log('processing boxes');
  for (let i = 0; i < boxes.length; i++) {
    if (suppress.has(i)) {
      continue;
    }

    selected.push(boxes[i]);

    for (let j = i + 1; j < boxes.length; j++) {
      if (suppress.has(j)) {
        continue;
      }

      const iou = calculateIoU(boxes[i].bbox, boxes[j].bbox);
      if (iou >= iouThreshold) {
        suppress.add(j);
      }
    }
  }

  return selected;
};

// Calculate IoU (Intersection over Union)
const calculateIoU = (box1: BBox, box2: BBox) => {
  const [x1, y1, w1, h1] = box1;
  const [x2, y2, w2, h2] = box2;

  const intersection = {
    left: Math.max(x1 - w1 / 2, x2 - w2 / 2),
    right: Math.min(x1 + w1 / 2, x2 + w2 / 2),
    top: Math.max(y1 - h1 / 2, y2 - h2 / 2),
    bottom: Math.min(y1 + h1 / 2, y2 + h2 / 2),
  };

  if (
    intersection.right < intersection.left ||
    intersection.bottom < intersection.top
  ) {
    return 0;
  }

  const intersectionArea =
    (intersection.right - intersection.left) *
    (intersection.bottom - intersection.top);
  const box1Area = w1 * h1;
  const box2Area = w2 * h2;

  return intersectionArea / (box1Area + box2Area - intersectionArea);
};