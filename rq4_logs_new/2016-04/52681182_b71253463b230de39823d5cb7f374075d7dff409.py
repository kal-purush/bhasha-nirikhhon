import SmartSprinklerModule as SSM
import math

# Initialize the position of the sprinkler to be at -90 degrees in pan, and -45 degrees in tilt
pan_angle = SSM.rotate_motor('pan', -.9)
tilt_angle = SSM.rotate_motor('tilt', -1.1)

image = SSM.capture_image(3,3,'target_check')
flame_present, x_centroid, y_centroid, edge_crossing = SSM.find_centroid(image)
if not flame_present:
    print 'Please adjust flame or camera until flame is within camera frame'
    exit()
elif edge_crossing is not 0:
    print 'Flame touches edge, please adjust'
    exit()
else:
    initial_tilt = tilt_angle
    angle_change = 3 * math.pi / 180
    print 'Flame detected!'
    i = 1
    while flame_present:
        tilt_angle = SSM.rotate_motor('tilt', tilt_angle - angle_change)
        image = SSM.capture_image(3,3, 'neg_calib ' + str(i))
        flame_present, x_centroid, y_centroid, edge_crossing = SSM.find_centroid(image)
        i = i + 1
    tilt_min = tilt_angle
    tilt_angle = SSM.rotate_motor('tilt', initial_tilt)
    flame_present = 1
    j = 1
    while flame_present:
        tilt_angle = SSM.rotate_motor('tilt', tilt_angle + angle_change)
        image = SSM.capture_image(3,3, 'pos_calib ' + str(j))
        flame_present, x_centroid, y_centroid, edge_crossing = SSM.find_centroid(image)
        j = j + 1
    tilt_max = tilt_angle

    print 'tilt min ' + str(tilt_min)
    print 'tilt max ' + str(tilt_max)

    SSM.rotate_motor('tilt', tilt_min + abs((tilt_max - tilt_min)/2))
    image = SSM.capture_image(3,3, 'centered_image')
    flame_present, x_centroid, y_centroid, edge_crossing = SSM.find_centroid(image)
    print 'yc ' + str(y_centroid)