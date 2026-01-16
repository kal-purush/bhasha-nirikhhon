import cv2



cap = cv2.VideoCapture("video_test.mp4")
frame_width = int(cap.get(3))
frame_height = int(cap.get(4))

fourcc = cv2.VideoWriter_fourcc('M', 'J', 'P', 'G')
out = cv2.VideoWriter("normal.avi", fourcc, 30, (frame_width, frame_height))



ok, img = cap.read()

while ok:

    ok, frame = cap.read()

    if not ok:
        print("End of video")
        break

    out.write(frame)

    cv2.imshow("Frame", frame)
    cv2.waitKey(1)

cap.release()
out.release()
cv2.destroyAllWindows()