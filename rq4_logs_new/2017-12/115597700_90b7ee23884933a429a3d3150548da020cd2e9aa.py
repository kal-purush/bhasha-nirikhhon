from lesson_functions import *
from scipy.ndimage.measurements import label
from sklearn.externals import joblib
import matplotlib.pyplot as plt
import pickle

def bin_to_rgb(bin_image):
    return cv2.cvtColor(bin_image * 255, cv2.COLOR_GRAY2BGR)


def compose_images(dst_image, src_image, split_rows, split_columns, which_section):
    assert 0 < which_section <= split_rows * split_columns

    if split_rows > split_columns:
        newH = int(dst_image.shape[0] / split_rows)
        dim = (int(dst_image.shape[1] * newH / dst_image.shape[0]), newH)
    else:
        newW = int(dst_image.shape[1] / split_columns)
        dim = (newW, int(dst_image.shape[0] * newW / dst_image.shape[1]))

    if len(src_image.shape) == 2:
        srcN = bin_to_rgb(src_image)
    else:
        srcN = np.copy(src_image)

    img = cv2.resize(srcN, dim, interpolation=cv2.INTER_AREA)
    nr = (which_section - 1) // split_columns
    nc = (which_section - 1) % split_columns
    dst_image[nr * img.shape[0]:(nr + 1) * img.shape[0], nc * img.shape[1]:(nc + 1) * img.shape[1]] = img
    return dst_image


def find_cars(img, ystart, ystop, scale, svc, X_scaler, orient, pix_per_cell, cell_per_block, spatial_size, hist_bins,
              window, cells_per_step):
    test_features = []
    box = []

    draw_img = np.copy(img)
    img = img.astype(np.float32) / 255

    img_tosearch = img[ystart:ystop, :, :]
    ctrans_tosearch = convert_color(img_tosearch, conv='RGB2YCrCb')

    if scale != 1:
        imshape = ctrans_tosearch.shape
        ctrans_tosearch = cv2.resize(ctrans_tosearch, (np.int(imshape[1] / scale), np.int(imshape[0] / scale)))

    ch1 = ctrans_tosearch[:, :, 0]
    ch2 = ctrans_tosearch[:, :, 1]
    ch3 = ctrans_tosearch[:, :, 2]

    # Define blocks and steps as above
    nxblocks = (ch1.shape[1] // pix_per_cell) - cell_per_block + 1
    nyblocks = (ch1.shape[0] // pix_per_cell) - cell_per_block + 1
    nfeat_per_block = orient * cell_per_block ** 2

    nblocks_per_window = (window // pix_per_cell) - cell_per_block + 1
    nxsteps = (nxblocks - nblocks_per_window) // cells_per_step
    nysteps = (nyblocks - nblocks_per_window) // cells_per_step

    # Compute individual channel HOG features for the entire image
    hog1 = get_hog_features(ch1, orient, pix_per_cell, cell_per_block, vis=False, feature_vec=False)
    hog2 = get_hog_features(ch2, orient, pix_per_cell, cell_per_block, vis=False, feature_vec=False)
    hog3 = get_hog_features(ch3, orient, pix_per_cell, cell_per_block, vis=False, feature_vec=False)

    for xb in range(nxsteps):
        for yb in range(nysteps):
            ypos = yb * cells_per_step
            xpos = xb * cells_per_step
            # Extract HOG for this patch
            hog_feat1 = hog1[ypos:ypos + nblocks_per_window, xpos:xpos + nblocks_per_window].ravel()
            hog_feat2 = hog2[ypos:ypos + nblocks_per_window, xpos:xpos + nblocks_per_window].ravel()
            hog_feat3 = hog3[ypos:ypos + nblocks_per_window, xpos:xpos + nblocks_per_window].ravel()
            hog_features = np.hstack((hog_feat1, hog_feat2, hog_feat3))

            xleft = xpos * pix_per_cell
            ytop = ypos * pix_per_cell

            # Extract the image patch
            subimg = cv2.resize(ctrans_tosearch[ytop:ytop + window, xleft:xleft + window], (64, 64))

            # Get color features
            spatial_features = bin_spatial(subimg, size=spatial_size)
            hist_features = color_hist(subimg, nbins=hist_bins)

            # Scale features and make a prediction
            test_features = X_scaler.transform(
                np.hstack((spatial_features, hist_features, hog_features)).reshape(1, -1), y=None, copy=None)
            # test_features = X_scaler.transform(np.hstack((shape_feat, hist_feat)).reshape(1, -1))
            test_prediction = svc.predict(test_features)

            if test_prediction == 1:
                xbox_left = np.int(xleft * scale)
                ytop_draw = np.int(ytop * scale)
                win_draw = np.int(window * scale)
                cv2.rectangle(draw_img, (xbox_left, ytop_draw + ystart),
                              (xbox_left + win_draw, ytop_draw + win_draw + ystart), (0, 0, 255), 6)
                box.append([(xbox_left, ytop_draw + ystart), (xbox_left + win_draw, ytop_draw + win_draw + ystart)])

    return draw_img, box


def object_detection(image, svc, X_scaler, orient, pix_per_cell, cell_per_block, spatial_size, hist_bins,
                     box_tmp_1, box_tmp_2, box_tmp_3, box_tmp_4):

    #######################################################################################################
    image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
    showed_image = np.zeros_like(image)

    ystart = 400
    ystop = 656

    scale = 1.5
    window = 64
    cells_per_step = 1
    out_img, boxes_1 = find_cars(image, ystart, ystop, scale, svc, X_scaler, orient, pix_per_cell,
                                 cell_per_block,
                                 spatial_size, hist_bins, window, cells_per_step)

    scale = 1
    window = 64
    cells_per_step = 1
    out_img, boxes_2 = find_cars(image, ystart, ystop, scale, svc, X_scaler, orient, pix_per_cell,
                                 cell_per_block,
                                 spatial_size, hist_bins, window, cells_per_step)

    scale = 2
    window = 128
    cells_per_step = 1
    out_img, boxes_3 = find_cars(image, ystart, ystop, scale, svc, X_scaler, orient, pix_per_cell,
                                 cell_per_block,
                                 spatial_size, hist_bins, window, cells_per_step)

    boxes = boxes_1 + boxes_2 + boxes_3

    #######################################################################################################

    # Read in a pickle file with bboxes saved
    # Each item in the "all_bboxes" list will contain a
    # list of boxes for one of the images shown above
    box_list = boxes
    box_tmp_5 = box_tmp_4
    box_tmp_4 = box_tmp_3
    box_tmp_3 = box_tmp_2
    box_tmp_2 = box_tmp_1
    box_tmp_1 = box_list
    box_tmp = box_tmp_5 + box_tmp_4 + box_tmp_3 + box_tmp_2 + box_tmp_1

    # Read in image similar to one shown above
    heat = np.zeros_like(image[:, :, 0]).astype(np.float)

    # Add heat to each box in box list
    heat = add_heat(heat, box_tmp)

    # Apply threshold to help remove false positives
    heat = apply_threshold(heat, 10)

    # Visualize the heatmap when displaying
    heatmap = np.clip(heat, 0, 255)
    # heatmap_imposeed = cv2.addWeighted(image, 1, heatmap, 0.3, 0)
    # compose_images(showed_image, heatmap_imposeed, 2, 2, 1)

    # Find final boxes from heatmap using label function
    labels = label(heatmap)
    draw_img = draw_labeled_bboxes(np.copy(image), labels)
    compose_images(showed_image, draw_img, 2, 2, 2)

    return draw_img, showed_image


def lane_recognition(image, mtx, dist):


    #######################################################################################################
    undist = cv2.undistort(cv2.cvtColor(image, cv2.COLOR_BGR2RGB), mtx, dist, None, mtx)
    showed_image = np.zeros_like(undist)
    # compose_images(showed_image, undist, 2, 2, 1)

    #######################################################################################################
    image = undist

    # Convert to HLS color space and separate the S channel
    # Note: img is the undistorted image
    hls = cv2.cvtColor(image, cv2.COLOR_RGB2HLS)
    s_channel = hls[:, :, 2]

    # Grayscale image
    # NOTE: we already saw that standard grayscaling lost color information for the lane lines
    # Explore gradients in other colors spaces / color channels to see what might work better
    gray = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)

    # Sobel x
    sobelx = cv2.Sobel(gray, cv2.CV_64F, 1, 0)  # Take the derivative in x
    abs_sobelx = np.absolute(sobelx)  # Absolute x derivative to accentuate lines away from horizontal
    scaled_sobel = np.uint8(255 * abs_sobelx / np.max(abs_sobelx))

    # Threshold x gradient
    thresh_min = 20
    thresh_max = 100
    sxbinary = np.zeros_like(scaled_sobel)
    sxbinary[(scaled_sobel >= thresh_min) & (scaled_sobel <= thresh_max)] = 1

    # Threshold color channel
    s_thresh_min = 90
    s_thresh_max = 255
    s_binary = np.zeros_like(s_channel)
    s_binary[(s_channel >= s_thresh_min) & (s_channel <= s_thresh_max)] = 1

    hsv = cv2.cvtColor(image, cv2.COLOR_RGB2HSV)
    yellow = cv2.inRange(hsv, (20, 100, 100), (50, 255, 255))

    sensitivity_1 = 68
    white = cv2.inRange(hsv, (0, 0, 255 - sensitivity_1), (255, 20, 255))

    sensitivity_2 = 60
    white_2 = cv2.inRange(hls, (0, 255 - sensitivity_2, 0), (255, 255, sensitivity_2))
    white_3 = cv2.inRange(undist, (200, 200, 200), (255, 255, 255))

    # Stack each channel to view their individual contributions in green and blue respectively
    # This returns a stack of the two binary images, whose components you can see as different colors
    color_binary = np.dstack((np.zeros_like(sxbinary), sxbinary, s_binary))

    # Combine the two binary thresholds
    combined_binary = np.zeros_like(sxbinary)
    combined_binary[
        (s_binary == 1) | (sxbinary == 1) | (yellow == 1) | (white == 1) | (white_2 == 1) | (white_3 == 1)] = 1

    #######################################################################################################
    image = combined_binary

    src = np.float32(
        [[740, 450],
         [940, 680],
         [340, 680],
         [540, 450]])

    dst = np.float32(
        [[1000, 100],
         [1000, 700],
         [350, 700],
         [350, 100]])

    def warp(img, src, dst):

        img_size = (img.shape[1], img.shape[0])

        M = cv2.getPerspectiveTransform(src, dst)
        Minv = cv2.getPerspectiveTransform(dst, src)
        warped = cv2.warpPerspective(img, M, img_size, flags=cv2.INTER_LINEAR)

        return warped

    warped_im = warp(image, src, dst)
    warped_showed = warp(undist, src, dst)
    compose_images(showed_image, warped_showed, 2, 2, 1)

    #######################################################################################################
    binary_warped = warped_im

    # Assuming you have created a warped binary image called "binary_warped"
    # Take a histogram of the bottom half of the image
    histogram = np.sum(binary_warped[int(binary_warped.shape[0] / 2):, :], axis=0)
    # Create an output image to draw on and  visualize the result
    out_img = np.dstack((binary_warped, binary_warped, binary_warped)) * 255
    # Find the peak of the left and right halves of the histogram
    # These will be the starting point for the left and right lines
    midpoint = np.int(histogram.shape[0] / 2)
    leftx_base = np.argmax(histogram[:midpoint])
    rightx_base = np.argmax(histogram[midpoint:]) + midpoint

    # Choose the number of sliding windows
    nwindows = 9
    # Set height of windows
    window_height = np.int(binary_warped.shape[0] / nwindows)
    # Identify the x and y positions of all nonzero pixels in the image
    nonzero = binary_warped.nonzero()
    nonzeroy = np.array(nonzero[0])
    nonzerox = np.array(nonzero[1])
    # Current positions to be updated for each window
    leftx_current = leftx_base
    rightx_current = rightx_base
    # Set the width of the windows +/- margin
    margin = 100
    # Set minimum number of pixels found to recenter window
    minpix = 50
    # Create empty lists to receive left and right lane pixel indices
    left_lane_inds = []
    right_lane_inds = []

    # Step through the windows one by one
    for window in range(nwindows):
        # Identify window boundaries in x and y (and right and left)
        win_y_low = binary_warped.shape[0] - (window + 1) * window_height
        win_y_high = binary_warped.shape[0] - window * window_height
        win_xleft_low = leftx_current - margin
        win_xleft_high = leftx_current + margin
        win_xright_low = rightx_current - margin
        win_xright_high = rightx_current + margin
        # Draw the windows on the visualization image
        cv2.rectangle(out_img, (win_xleft_low, win_y_low), (win_xleft_high, win_y_high), (0, 255, 0), 2)
        cv2.rectangle(out_img, (win_xright_low, win_y_low), (win_xright_high, win_y_high), (0, 255, 0), 2)
        # Identify the nonzero pixels in x and y within the window
        good_left_inds = ((nonzeroy >= win_y_low) & (nonzeroy < win_y_high) & (nonzerox >= win_xleft_low) & (
        nonzerox < win_xleft_high)).nonzero()[0]
        good_right_inds = ((nonzeroy >= win_y_low) & (nonzeroy < win_y_high) & (nonzerox >= win_xright_low) & (
        nonzerox < win_xright_high)).nonzero()[0]
        # Append these indices to the lists
        left_lane_inds.append(good_left_inds)
        right_lane_inds.append(good_right_inds)
        # If you found > minpix pixels, recenter next window on their mean position
        if len(good_left_inds) > minpix:
            leftx_current = np.int(np.mean(nonzerox[good_left_inds]))
        if len(good_right_inds) > minpix:
            rightx_current = np.int(np.mean(nonzerox[good_right_inds]))

    # Concatenate the arrays of indices
    left_lane_inds = np.concatenate(left_lane_inds)
    right_lane_inds = np.concatenate(right_lane_inds)

    # Extract left and right line pixel positions
    leftx = nonzerox[left_lane_inds]
    lefty = nonzeroy[left_lane_inds]
    rightx = nonzerox[right_lane_inds]
    righty = nonzeroy[right_lane_inds]

    # Fit a second order polynomial to each
    left_fit = np.polyfit(lefty, leftx, 2)
    right_fit = np.polyfit(righty, rightx, 2)

    # Visualization

    # Generate x and y values for plotting
    ploty = np.linspace(0, binary_warped.shape[0] - 1, binary_warped.shape[0])
    left_fitx = left_fit[0] * ploty ** 2 + left_fit[1] * ploty + left_fit[2]
    right_fitx = right_fit[0] * ploty ** 2 + right_fit[1] * ploty + right_fit[2]

    out_img[nonzeroy[left_lane_inds], nonzerox[left_lane_inds]] = [255, 0, 0]
    out_img[nonzeroy[right_lane_inds], nonzerox[right_lane_inds]] = [0, 0, 255]

    #######################################################################################################

    # Assume you now have a new warped binary image
    # from the next frame of video (also called "binary_warped")
    # It's now much easier to find line pixels!
    nonzero = binary_warped.nonzero()
    nonzeroy = np.array(nonzero[0])
    nonzerox = np.array(nonzero[1])
    margin = 100
    left_lane_inds = (
    (nonzerox > (left_fit[0] * (nonzeroy ** 2) + left_fit[1] * nonzeroy + left_fit[2] - margin)) & (
    nonzerox < (left_fit[0] * (nonzeroy ** 2) + left_fit[1] * nonzeroy + left_fit[2] + margin)))
    right_lane_inds = (
    (nonzerox > (right_fit[0] * (nonzeroy ** 2) + right_fit[1] * nonzeroy + right_fit[2] - margin)) & (
    nonzerox < (right_fit[0] * (nonzeroy ** 2) + right_fit[1] * nonzeroy + right_fit[2] + margin)))

    # Again, extract left and right line pixel positions
    leftx = nonzerox[left_lane_inds]
    lefty = nonzeroy[left_lane_inds]
    rightx = nonzerox[right_lane_inds]
    righty = nonzeroy[right_lane_inds]

    # Fit a second order polynomial to each
    if (lefty.size != 0) & (leftx.size != 0):
        left_fit = np.polyfit(lefty, leftx, 2)
    else:
        left_fit = np.zeros(2)
    if (righty.size != 0) & (rightx.size != 0):
        right_fit = np.polyfit(righty, rightx, 2)
    else:
        right_fit = np.zeros(2)

    # Generate x and y values for plotting
    ploty = np.linspace(0, binary_warped.shape[0] - 1, binary_warped.shape[0])
    left_fitx = left_fit[0] * ploty ** 2 + left_fit[1] * ploty + left_fit[2]
    right_fitx = right_fit[0] * ploty ** 2 + right_fit[1] * ploty + right_fit[2]

    # Create an image to draw on and an image to show the selection window
    out_img = np.dstack((binary_warped, binary_warped, binary_warped)) * 255
    window_img = np.zeros_like(out_img)
    # Color in left and right line pixels
    out_img[nonzeroy[left_lane_inds], nonzerox[left_lane_inds]] = [255, 0, 0]
    out_img[nonzeroy[right_lane_inds], nonzerox[right_lane_inds]] = [0, 0, 255]

    # Visualization

    # Generate a polygon to illustrate the search window area
    # And recast the x and y points into usable format for cv2.fillPoly()
    left_line_window1 = np.array([np.transpose(np.vstack([left_fitx - margin, ploty]))])
    left_line_window2 = np.array([np.flipud(np.transpose(np.vstack([left_fitx + margin, ploty])))])
    left_line_pts = np.hstack((left_line_window1, left_line_window2))
    right_line_window1 = np.array([np.transpose(np.vstack([right_fitx - margin, ploty]))])
    right_line_window2 = np.array([np.flipud(np.transpose(np.vstack([right_fitx + margin, ploty])))])
    right_line_pts = np.hstack((right_line_window1, right_line_window2))

    # Draw the lane onto the warped blank image
    cv2.fillPoly(window_img, np.int_([left_line_pts]), (0, 255, 0))
    cv2.fillPoly(window_img, np.int_([right_line_pts]), (0, 255, 0))
    result_1 = cv2.addWeighted(out_img, 1, window_img, 0.3, 0)

    #######################################################################################################

    # Define y-value where we want radius of curvature
    # I'll choose the maximum y-value, corresponding to the bottom of the image
    ploty = np.linspace(0, 719, num=720)  # to cover same y-range as image
    y_eval = np.max(ploty)
    left_curverad = ((1 + (2 * left_fit[0] * y_eval + left_fit[1]) ** 2) ** 1.5) / np.absolute(2 * left_fit[0])
    right_curverad = ((1 + (2 * right_fit[0] * y_eval + right_fit[1]) ** 2) ** 1.5) / np.absolute(
        2 * right_fit[0])

    # Define conversions in x and y from pixels space to meters
    ym_per_pix = 30 / 720  # meters per pixel in y dimension
    xm_per_pix = 3.7 / 700  # meters per pixel in x dimension

    # Fit new polynomials to x,y in world space
    # left_fit_cr = np.polyfit(ploty*ym_per_pix, leftx*xm_per_pix, 2)
    # right_fit_cr = np.polyfit(ploty*ym_per_pix, rightx*xm_per_pix, 2)
    left_fit_cr = np.polyfit(ploty * ym_per_pix, left_fitx * xm_per_pix, 2)
    right_fit_cr = np.polyfit(ploty * ym_per_pix, right_fitx * xm_per_pix, 2)
    # Calculate the new radii of curvature
    left_curverad = ((1 + (
    2 * left_fit_cr[0] * y_eval * ym_per_pix + left_fit_cr[1]) ** 2) ** 1.5) / np.absolute(2 * left_fit_cr[0])
    right_curverad = ((1 + (
    2 * right_fit_cr[0] * y_eval * ym_per_pix + right_fit_cr[1]) ** 2) ** 1.5) / np.absolute(
        2 * right_fit_cr[0])

    # Calcurate offset from the lane
    left = left_fitx[len(left_fitx) - 1] * xm_per_pix
    right = right_fitx[len(right_fitx) - 1] * xm_per_pix
    center = (left_fitx[len(left_fitx) - 1] * xm_per_pix + right_fitx[len(right_fitx) - 1] * xm_per_pix) / 2
    vehicle_pos = 1280 / 2 * xm_per_pix
    vehicle_pos_center = vehicle_pos - center

    #######################################################################################################

    # Create an image to draw the lines on
    # warp_zero = np.zeros_like(warped).astype(np.uint8)
    warp_zero = np.zeros_like(binary_warped).astype(np.uint8)
    color_warp = np.dstack((warp_zero, warp_zero, warp_zero))

    # Recast the x and y points into usable format for cv2.fillPoly()
    pts_left = np.array([np.transpose(np.vstack([left_fitx, ploty]))])
    pts_right = np.array([np.flipud(np.transpose(np.vstack([right_fitx, ploty])))])
    pts = np.hstack((pts_left, pts_right))

    # Draw the lane onto the warped blank image
    cv2.fillPoly(color_warp, np.int_([pts]), (0, 255, 0))

    Minv = cv2.getPerspectiveTransform(dst, src)

    # Warp the blank back to original image space using inverse perspective matrix (Minv)
    newwarp = cv2.warpPerspective(color_warp, Minv, (image.shape[1], image.shape[0]))

    if vehicle_pos_center >= 0:
        showed_curverad = right_curverad
    else:
        showed_curverad = left_curverad

    font = cv2.FONT_HERSHEY_SIMPLEX
    text_1 = 'Radius of Curvature : ' + str(int(showed_curverad)) + 'm'
    cv2.putText(undist, text_1, (100, 100), font, 2, (255, 255, 255))
    text_2 = 'Offset from Center : ' + str(round(vehicle_pos_center, 3)) + 'm'
    cv2.putText(undist, text_2, (100, 200), font, 2, (255, 255, 255))

    # Combine the result with the original image
    result_2 = cv2.addWeighted(undist, 1, newwarp, 0.3, 0)

    #######################################################################################################
    # showed_image = result_1
    # showed_image = result_2
    compose_images(showed_image, result_1, 2, 2, 2)
    compose_images(showed_image, result_2, 2, 2, 3)

    return newwarp, showed_image

class ObjectDetector(object):
    def __init__(self):

        # init for obeject detection
        svc, X_scaler, orient, pix_per_cell, cell_per_block, spatial_size, hist_bins = joblib.load("vehicle_detector_tmp.pkl")
        self.box_tmp_1 = []
        self.box_tmp_2 = []
        self.box_tmp_3 = []
        self.box_tmp_4 = []
        self.box_tmp_5 = []
        self.box_tmp = []
        self.boxes = []
        self.heat = []
        self.svc = svc
        self.X_scaler = X_scaler
        self.orient = orient
        self.pix_per_cell = pix_per_cell
        self.cell_per_block = cell_per_block
        self.spatial_size = spatial_size
        self.hist_bins = hist_bins

        # init for lane recognition
        dist_pickle = pickle.load(open("./dist_pickle.p", "rb"))
        self.mtx = dist_pickle["mtx"]
        self.dist = dist_pickle["dist"]

    def process_image(self, image):

        object_detection_result, object_detection_result_composed = object_detection(image, self.svc, self.X_scaler, self.orient, self.pix_per_cell, self.cell_per_block, self.spatial_size, self.hist_bins,
                         self.box_tmp_1, self.box_tmp_2, self.box_tmp_3, self.box_tmp_4)

        lane_recognition_result, lane_recognition_result_composed = lane_recognition(image, self.mtx, self.dist)

        showed_image = np.zeros_like(image)
        compose_images(showed_image, object_detection_result_composed, 2, 2, 1)
        compose_images(showed_image, lane_recognition_result_composed, 2, 2, 2)

        obj_lane = cv2.addWeighted(object_detection_result, 1, lane_recognition_result, 0.3, 0)
        compose_images(showed_image, obj_lane, 2, 2, 3)

        #######################################################################################################
        cv2.imwrite("./video_images.jpg", showed_image)

        return cv2.cvtColor(showed_image, cv2.COLOR_BGR2RGB)