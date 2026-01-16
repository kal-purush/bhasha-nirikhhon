def center_target(pan_angle, tilt_angle, cx, cy, initial_rotation=2*math.pi/180):
    # This function commands the motors to adjust the camera until the centroid of the fire is brought to the center of
    # the image
    # pan_angle and tilt_angle indicate the current pan and tilt angles of the camera
    # initial_rotation indicates the angle by which the camera will rotate to calibrate its gains

    # The function returns 1 if successful, and 0 if unsuccessful

    # Define the image size
    x_max = 640
    y_max = 480

    # Capture an initial image
    #img = capture_image()
    #flame, cx, cy = find_centroid(img)

    # Calculate how far the x and y coordinates of the centroid are from the center of the image
    x_offset = float(x_max) / 2.0 - cx
    y_offset = float(y_max) / 2.0 - cy

    print 'the x offset is ' + str(x_offset)
    print 'the y offset is ' + str(y_offset)

    # Define a tolerance of how close you want cx and cy to be to the center of the image (in pixels)
    tolerance = 3

    # Do an initial rotation to calibrate gains
    if abs(x_offset) > tolerance:
        if x_offset < 0:
            pan_angle = rotate_motor('pan', pan_angle - initial_rotation)
        else:
            pan_angle = rotate_motor('pan', pan_angle + initial_rotation)
    if abs(y_offset) > tolerance:
        if y_offset < 0:
            tilt_angle = rotate_motor('tilt', tilt_angle + initial_rotation)
        else:
            tilt_angle = rotate_motor('tilt', tilt_angle - initial_rotation)
    i = 1
    img = capture_image(10,10,'centerimage' + str(i))
    flame, cx, cy = find_centroid(img)
    if not flame:
        print 'Fire no longer detected after small angle change. Either the camera is too close to the fire, or the' \
              ' fire has gone out'
        exit()

    new_x_offset = float(x_max) / 2.0 - cx
    new_y_offset = float(y_max) / 2.0 - cy

    # Calculate gains. Gains refer to how far the camera moved versus how many pixels the centroid coordinates changed
    # by.
    x_change = new_x_offset - x_offset
    y_change = new_y_offset - y_offset
    if x_change == 0 or y_change == 0:
        print('The initial turret rotation is too small. Please choose a larger initial rotation.')
        exit()

    x_gain = .1 * initial_rotation / abs(x_change)
    y_gain = .1 * initial_rotation / abs(y_change)

    x_offset = new_x_offset
    y_offset = new_y_offset

    # Repeat this process, updating the gains every iteration, until convergence
    while abs(x_offset) > tolerance or abs(y_offset) > tolerance:
        print 'the x offset is ' + str(x_offset)
        print 'the y offset is ' + str(y_offset)
        if abs(x_offset) > tolerance:
            if x_offset < 0:
                rotate_motor('pan', pan_angle - abs(x_offset) * x_gain)
            else:
                rotate_motor('pan', pan_angle + x_offset * x_gain)
        if abs(y_offset) > tolerance:
            if y_offset < 0:
                rotate_motor('tilt', tilt_angle + abs(y_offset) * y_gain)
            else:
                rotate_motor('tilt', tilt_angle - y_offset * y_gain)
        i = i + 1
        img = capture_image(10,10,'centerimage' + str(i))
        flame, cx, cy = find_centroid(img)
        if not flame:
            print 'Flame lost'
            return 0

        new_x_offset = float(x_max) / 2.0 - cx
        new_y_offset = float(y_max) / 2.0 - cy

        x_change = new_x_offset - x_offset
        y_change = new_y_offset - y_offset

        if x_change == 0 or y_change == 0:
            print 'The gain is too small. Consider adding higher proportional or integral gains'
            exit()

        x_gain = .1 * abs(x_offset * x_gain) / abs(x_change)
        y_gain = .1 * abs(y_offset * y_gain) / abs(y_change)

        x_offset = new_x_offset
        y_offset = new_y_offset

    return 1