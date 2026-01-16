import cv2
import numpy as np

def find_conturs(image):
    """
    Find receipt contours

    Returns None if no contours were found
    """
    # detect edges
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (7, 7), 0)
    edges = cv2.Canny(gray, 50, 100)
    cv2.waitKey(0)
    # detect contours and sort them according to contour area
    _, contours, _ = cv2.findContours(edges, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)
    contours = sorted(contours, key=cv2.contourArea, reverse=True)[:5]
    
    receipt_contour = None
    for contour in contours:
        perimeter = cv2.arcLength(contour, True)
        # approximate a polygonal curve with the specified precision
        # function uses Ramer–Douglas–Peucker algorithm
        approx_curve = cv2.approxPolyDP(contour, 0.02 * perimeter, True) # contour, epsilon, closed? 
        if len(approx_curve) == 4:
            receipt_contour = approx_curve
            break
    return receipt_contour

def order_points(points):
    """
    Returns a numpy array of `points` ordered like: 
    top-left, top-right, bottom-right, bottom-left
    """
    rectangle = np.zeros((4, 2), dtype="float32")

    # the top-left point will have the smallest sum
    # the bottom-right point will have the largest sum
    s = points.sum(axis=1)
    rectangle[0] = points[np.argmin(s)]
    rectangle[2] = points[np.argmax(s)]

    # top-right point will have the smallest difference,
    # bottom-left will have the largest difference
    diff = np.diff(points, axis=1)
    rectangle[1] = points[np.argmin(diff)]
    rectangle[3] = points[np.argmax(diff)]

    return rectangle

def four_point_transform(image, points):

    rectangle = order_points(points)
    (tl, tr, br, bl) = rectangle

    # width of the image is equal to the distance between right
    # and left corners
    # calculate width for top and bottom corners and
    # then find maximum
    w1 = np.sqrt(((br[0] - bl[0]) ** 2) + ((br[1] - bl[1]) ** 2))
    w2 = np.sqrt(((tr[0] - tl[0]) ** 2) + ((tr[1] - tl[1]) ** 2))
    width = max(int(w1), int(w2))

    # height is computed in a similar way
    h1 = np.sqrt(((tr[0] - br[0]) ** 2) + ((tr[1] - br[1]) ** 2))
    h2 = np.sqrt(((tl[0] - bl[0]) ** 2) + ((tl[1] - bl[1]) ** 2))
    height = max(int(h1), int(h2))

    # now that we have the dimensions of the new image, construct
    # the set of destination points to obtain a "birds eye view",
    # (i.e. top-down view) of the image, again specifying points
    # in the top-left, top-right, bottom-right, and bottom-left
    # order

    # having the dimensions of cropped image we have to create 
    # `dst` - destination points for the image
    dst = np.array([
        [0, 0],
        [width - 1, 0],
        [width - 1, height - 1],
        [0, height - 1]], dtype="float32")

    M = cv2.getPerspectiveTransform(rectangle, dst)
    return cv2.warpPerspective(image, M, (width, height))

def crop(image):
    """
    If possible cut out receipt from raw `image`. 

    Returns cropped image or `None`
    """
    contours = find_conturs(image)
    if type(contours) is np.ndarray:
        points = np.array(contours.reshape(4, 2))
        cropped = four_point_transform(image, points)
        return cropped
    return None

# if __name__ == "__main__":

#     TEST1 = "../../../test_images/rot2.jpeg"
#     TEST2 = "../../../test_images/polo2.jpg"
#     img = cv2.imread(TEST2)
#     c = crop(img)
#     if c is not None: 
#         cv2.imshow("dfd", c)
#     cv2.imshow("img", img)
#     cv2.waitKey(0)
    