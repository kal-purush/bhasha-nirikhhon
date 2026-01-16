import pickle
import numpy as np
from utils import *
from scipy.ndimage.measurements import label
from moviepy.editor import VideoFileClip
from vehicle import Vehicle


svm_model = pickle.load(open("svm_model.p", "rb"))
svc = svm_model["svc"]
X_scaler = svm_model["X_scaler"]


color_space = hog_params["color_space"]
conv_color = hog_params["conv_color"]
orient = hog_params["orient"]
pix_per_cell = hog_params["pix_per_cell"]
cell_per_block = hog_params["cell_per_block"]
hog_channel = hog_params["hog_channel"]
spatial_size = hog_params["spatial_size"]
hist_bins = hog_params["hist_bins"]
spatial_feat = hog_params["spatial_feat"]
hist_feat = hog_params["hist_feat"]
hog_feat = hog_params["hog_feat"]


def find_cars(img, scale, ystart, ystop):
    heatmap = np.zeros_like(img[: ,: ,0])
    img = img.astype(np.float32) / 255

    img_tosearch = img[ystart:ystop, :, :]
    ctrans_tosearch = convert_color(img_tosearch, conv_color)
    if scale != 1:
        imshape = ctrans_tosearch.shape
        ctrans_tosearch = cv2.resize(ctrans_tosearch, (np.int(imshape[1 ] /scale), (np.int(imshape[0 ] /scale))))

    ch1 = ctrans_tosearch[: ,: ,0]
    ch2 = ctrans_tosearch[: ,: ,1]
    ch3 = ctrans_tosearch[: ,: ,2]

    nxblocks = (ch1.shape[1] // pix_per_cell) - 1
    nyblocks = (ch1.shape[0] // pix_per_cell) - 1

    window = 64
    nblocks_per_window = (window // pix_per_cell) - 1
    cells_per_step = 2
    nxsteps = (nxblocks - nblocks_per_window) // cells_per_step
    nysteps = (nyblocks - nblocks_per_window) // cells_per_step

    hog1 = get_hog_features(ch1, orient, pix_per_cell, cell_per_block, feature_vec=False)
    hog2 = get_hog_features(ch2, orient, pix_per_cell, cell_per_block, feature_vec=False)
    hog3 = get_hog_features(ch3, orient, pix_per_cell, cell_per_block, feature_vec=False)

    for xb in range(nxsteps):
        for yb in range(nysteps):
            ypos = yb * cells_per_step
            xpos = xb * cells_per_step

            hog_feat1 = hog1[ypos:ypos +nblocks_per_window, xpos:xpos +nblocks_per_window].ravel()
            hog_feat2 = hog2[ypos:ypos +nblocks_per_window, xpos:xpos +nblocks_per_window].ravel()
            hog_feat3 = hog3[ypos:ypos +nblocks_per_window, xpos:xpos +nblocks_per_window].ravel()
            hog_features = np.hstack((hog_feat1, hog_feat2, hog_feat3))

            xleft = xpos * pix_per_cell
            ytop = ypos * pix_per_cell

            # Extract the image patch
            subimg = cv2.resize(ctrans_tosearch[ytop:ytop +window, xleft:xleft +window], (64, 64))

            # Get features
            spatial_features = bin_spatial(subimg, size=spatial_size)
            hist_features = color_hist(subimg, nbins=hist_bins)

            features = np.hstack((spatial_features, hist_features, hog_features)).reshape(1, -1)

            # Scale extracted features to be fed to classifier
            test_features = X_scaler.transform(features)
            # Predict using your classifier
            test_prediction = svc.predict(test_features)

            test_decision = svc.decision_function(test_features)

            if test_prediction == 1 and test_decision > 0.6:
                xbox_left = np.int(xleft * scale)
                ytop_draw = np.int(ytop * scale)
                win_draw = np.int(window * scale)

                heatmap[ytop_draw +ystart:ytop_draw +win_draw +ystart, xbox_left:xbox_left +win_draw] += 1

    return heatmap


scaled_regions = [(1.2, 380, 520), (1.5, 400, 600), (2, 400, 660)]
vehicles = []
heat_stack = []
def process_image(img):
    global heat_stack
    heat_map = np.zeros_like(img[:, :, 0])
    for scaled_region in scaled_regions:
        found_heat_map = find_cars(img, scaled_region[0], scaled_region[1], scaled_region[2])
        thresholded_heat = apply_threshold(found_heat_map, 3)
        heat_map += np.asarray(thresholded_heat)

    heat_stack.append(heat_map)
    heat_stack = heat_stack[-10:]

    mean_heats = np.mean(heat_stack, axis=0).astype(int)
    mean_heats = apply_threshold(mean_heats, 1)
    heat = np.clip(mean_heats, 0, 255)
    labels = label(heat)

    draw_img = get_labeled_bboxes(np.copy(img), labels)
    return draw_img


def get_labeled_bboxes(img, labels):
    global vehicles
    # Iterate through all detected cars
    for car_number in range(1, max((labels[1] + 1), len(vehicles))):
        # Find pixels with each car_number label value
        nonzero = (labels[0] == car_number).nonzero()
        # Identify x and y values of those pixels
        nonzeroy = np.array(nonzero[0])
        nonzerox = np.array(nonzero[1])
        if len(nonzerox) == 0:
            continue

        #search_key = (np.min(nonzerox), np.min(nonzeroy))

        w = np.max(nonzerox) - np.min(nonzerox)
        h = np.max(nonzeroy) - np.min(nonzeroy)

        if car_number > len(vehicles):
            if w > 40 and h > 40:
                v = Vehicle()
                v.update_detection(nonzerox, nonzeroy)
                vehicles.append(v)
                #print("new vehicle at", search_key)

        else:
            vehicles[car_number-1].update_detection(nonzerox, nonzeroy)
            #print("update vehicle at", search_key)

    obselete_vehicles = []
    for i in range(len(vehicles)):
        ret, bbox = vehicles[i].get_bbox()
        #print("Check", i, "got", ret)
        if ret is True:
            cv2.rectangle(img, bbox[0], bbox[1], (0, 0, 255), 6)
        else:
            obselete_vehicles.append(i)

    # Remove non-detected vehicles
    for remove_ind in obselete_vehicles:
        del vehicles[remove_ind]

    # Return the image
    return img


def process_video():
    video_file = 'project_video.mp4'
    track_output = 'tracked_' + video_file
    clip = VideoFileClip(video_file)
    track_clip = clip.fl_image(process_image)
    track_clip.write_videofile(track_output, audio=False)#, verbose=True, progress_bar=False)
    return


process_video()
# example_images = glob.glob('test_video_images/*.jpg')
# for img_src in example_images:
#     img = mpimg.imread(img_src)
#
#     out_img, heat_map = find_cars(img, scale)