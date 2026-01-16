import cv2
import sys


def capture_frame(url, output_path):
	cap = cv2.VideoCapture(url)

	if not cap.isOpened():
		print("Error opening video stream")
		return

	ret, frame = cap.read()

	if not ret:
		print("Error reading video stream")
	else:
		cv2.imwrite(output_path, frame)
		print(f"Video written to {output_path}")

	cap.release()


if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: capture.py <url> <output_path>")
		sys.exit(1)

	url = sys.argv[1]
	output_path = sys.argv[2]
	capture_frame(url, output_path)