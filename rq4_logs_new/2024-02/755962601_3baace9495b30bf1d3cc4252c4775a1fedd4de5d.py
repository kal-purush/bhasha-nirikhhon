import os
import cv2

def crop_and_resize_images(input_parent_dir, output_parent_dir, crop_x, crop_y, crop_width, crop_height, target_width, target_height):
    # Loop through all subdirectories in the input parent directory
    for subfolder in os.listdir(input_parent_dir):
        input_subfolder = os.path.join(input_parent_dir, subfolder)
        
        # Check if the item in the parent directory is a directory
        if os.path.isdir(input_subfolder):
            # Create the corresponding output subdirectory in the output parent directory
            output_subfolder = os.path.join(output_parent_dir, subfolder)
            if not os.path.exists(output_subfolder):
                os.makedirs(output_subfolder)

            # Loop through all files in the input subdirectory
            for filename in os.listdir(input_subfolder):
                if filename.endswith(('.jpg', '.jpeg', '.png', '.bmp')):  # Add more image extensions if needed
                    # Read the image
                    img_path = os.path.join(input_subfolder, filename)
                    image = cv2.imread(img_path)

                    # Perform cropping
                    cropped_image = image[crop_y:crop_y+crop_height, crop_x:crop_x+crop_width]

                    # Resize the cropped image
                    resized_image = cv2.resize(cropped_image, (target_width, target_height))

                    # Save the resized image to the output subdirectory
                    output_path = os.path.join(output_subfolder, f"{filename}")
                    cv2.imwrite(output_path, resized_image)
            print(f"Finished cropping and resizing images in {subfolder}")

# Example usage
input_parent_directory = "./Original"
output_parent_directory = "./Cropped"
crop_x = 71  # starting x-coordinate of the crop region
crop_y = 287  # starting y-coordinate of the crop region
crop_width = 2101  # width of the crop region
crop_height = 1227  # height of the crop region
target_width = 227  # width of the resized image
target_height = 277  # height of the resized image

crop_and_resize_images(input_parent_directory, output_parent_directory, crop_x, crop_y, crop_width, crop_height, target_width, target_height)