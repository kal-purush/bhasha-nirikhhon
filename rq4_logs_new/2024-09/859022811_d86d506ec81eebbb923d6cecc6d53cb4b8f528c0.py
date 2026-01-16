import cv2  # OpenCV library for image processing.
import numpy as np
from pyzbar.pyzbar import decode  # Function from the pyzbar library to decode barcodes and QR codes

def decoder(image):
    gray_img = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)  # Converts the loaded image from BGR color space to grayscale
    barcode = decode(gray_img)  # Uses the decode function from pyzbar to detect and decode any barcodes or QR codes in the grayscale image

    for obj in barcode:
        points = obj.polygon  # Retrieves the polygon points that outline the detected barcode
        (x, y, w, h) = obj.rect  # Retrieves the rectangle coordinates (x, y, width, height) that bound the detected barcode
        pts = np.array(points, np.int32)  # Converts the polygon points to a NumPy array of type int32
        pts = pts.reshape((-1, 1, 2))  # Reshapes the points array to a format suitable for drawing the polygon with OpenCV functions

        cv2.polylines(image, [pts], True, (0, 255, 0), 3)  # Draws the polygon on the original image in green color with a thickness of 3 pixels

        barcodeData = obj.data.decode("utf-8")  # Decodes the barcode data from the object and converts it to a UTF-8 string
        barcodeType = obj.type  # Extracts the barcode type from the object
        string = "Data: " + str(barcodeData) + " | Type: " + str(barcodeType)  # Creates a string containing the decoded data and barcode type
        cv2.putText(image, string, (x, y), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 0, 0), 2)  # Displays the string information (data and type) on the image

choice = int(input("1. Scan via image\n2. Scan via WebCam\nChoice: "))
if choice == 2:
    cap = cv2.VideoCapture(0)  # Captures video frames from the webcam
    while True:
        ret, image = cap.read()
        decoder(image)
        cv2.imshow("Image", image)  # Displays the processed frames with highlighted barcodes and decoded data
        code = cv2.waitKey(10)
        if code == ord("q"):  # Waits for a key press (q in this case) to terminate the webcam capture
            break
    cap.release()
    cv2.destroyAllWindows()
else:
    img_path = input("Enter Image Path: ")  # Prompts the user to enter the file path of the image containing the QR code
    img = cv2.imread(img_path)  # Loads the image specified by the img_path into a NumPy array named img
    detector = cv2.QRCodeDetector()  # Creates an instance of the QRCodeDetector class from OpenCV

    data, bbox, _ = detector.detectAndDecode(img)  # Detects and decodes QR codes within the image
    if bbox is not None:
        for i in range(len(bbox)):
            cv2.polylines(img, [np.int32(bbox)], True, (0, 255, 0), 2)
        print(f"Data: {data}")
        cv2.imshow("Image", img)
        cv2.waitKey(0)
        cv2.destroyAllWindows()
    else:
        print("QR Code not detected")