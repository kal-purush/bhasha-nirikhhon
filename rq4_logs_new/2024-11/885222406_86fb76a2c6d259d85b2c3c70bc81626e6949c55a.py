from PIL import Image, ImageFilter
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
import numpy as np
from PIL import Image
import random
import json

COLORS = {
    "water": (0, 0, 255),
    "sand": (194, 178, 128),
    "grass": (34, 139, 34),
    "forest": (34, 100, 34),
    "rock": (139, 137, 137),
    "snow": (255, 250, 250)
}



def get_color(height):
    # if height > 0.2 and height < 0.9 and slope > 0.45:
    #   return COLORS["rock"]
    if height <= 0.2:
        return COLORS["water"]
    elif height > 0.2 and height <= 0.225:
        return COLORS["sand"]
    elif height > 0.225 and height <= 0.45:
        return COLORS["grass"]
    elif height > 0.45 and height <= 0.85:
        return COLORS["forest"]
    elif height > 0.85 and height <= 0.9:
        return COLORS["rock"]
    elif height > 0.9:
        return COLORS["snow"]


def plot_heightmap_3d():
    # Load the heightmap image
    image = Image.open('image.png').convert("L")  # Convert to grayscale
    data = np.array(image) / 255.0  # Normalize height data to [0, 1]

    # Compute normals
    normal_x, normal_y, normal_z = compute_normals(data)

    slope = np.sqrt(normal_x**2 + normal_y**2)  # Compute the slope magnitude

    # Map height and slope to colors
    colored_image = np.zeros((data.shape[0], data.shape[1], 3), dtype=np.uint8)
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            height = data[i, j]
            slope_value = slope[i, j]
            color = get_color(height, slope_value)
            colored_image[i, j] = color

    # Save the colored heightmap as an image
    output_image = Image.fromarray(colored_image)
    output_image.save("colored_heightmap.png")

    # colored_image2 = np.zeros((data.shape[0], data.shape[1], 3), dtype=np.uint8)
    # for i in range(data.shape[0]):
    #     for j in range(data.shape[1]):
    #         colored_image2[i, j] = (normal_x[i, j], normal_y[i, j], normal_z[i, j])
    # output_image2 = Image.fromarray(colored_image2)
    # output_image2.save("colored_heightmap_normals.png")


def compute_normals(data):
    # Convert data to float32 to ensure calculations are done in float
    data = data.astype(np.float32)

    # count = 0
    # for i in range(data.shape[0]):
    #     for j in range(data.shape[1]):
    #         R = data[min(i + 1, data.shape[0] - 1), j]  # Right neighbor
    #         L = data[max(i - 1, 0), j]  # Left neighbor
    #         B = data[i, min(j + 1, data.shape[1] - 1)]  # Bottom neighbor
    #         T = data[i, max(j - 1, 0)]  # Top neighbor
    #         normal_x[i, j] = (R - L) * 0.5
    #         normal_y[i, j] = (B - T) * 0.5
    #         normal_z[i, j] = -1
    #
    #         count += 1
    #         if count % 10_000 == 0:
    #             print(f"{count}/{data.size}")

    # Right (R) and Left (L) neighbors
    R = np.roll(data, shift=-1, axis=0)
    L = np.roll(data, shift=1, axis=0)
    R[-1, :] = R[-2, :]  # Handle boundary condition for the last row
    L[0, :] = L[1, :]  # Handle boundary condition for the first row

    # Bottom (B) and Top (T) neighbors
    B = np.roll(data, shift=-1, axis=1)
    T = np.roll(data, shift=1, axis=1)
    B[:, -1] = B[:, -2]  # Handle boundary condition for the last column
    T[:, 0] = T[:, 1]  # Handle boundary condition for the first column

    # Calculate normal_x, normal_y, and normal_z
    normal_x = (R - L) * 0.5
    normal_y = (B - T) * 0.5
    normal_z = -np.ones_like(data)

    # Compute right, left, bottom, and top neighbors using slicing
    # R = data[1:-1, 2:]  # Right neighbor
    # L = data[1:-1, :-2]  # Left neighbor
    # B = data[2:, 1:-1]  # Bottom neighbor
    # T = data[:-2, 1:-1]  # Top neighbor
    #
    # # Fill in normal_x and normal_y only in the central region to avoid edges
    # normal_x[1:-1, 1:-1] = (R - L) * 0.5
    # normal_y[1:-1, 1:-1] = (B - T) * 0.5
    #
    # # Calculate the magnitude for normalization
    # norm = np.sqrt(normal_x ** 2 + normal_y ** 2 + normal_z ** 2)
    #
    # # Avoid division by zero by adding a small epsilon
    # norm = np.where(norm == 0, 1, norm)
    #
    # # Normalize each component by dividing by the magnitude
    # normal_x /= norm
    # normal_y /= norm
    # normal_z /= norm

    return normal_x, normal_y, normal_z
def is_color_close(pixel, target_color, tolerance=10):
    """Check if a pixel is close to a target color within a tolerance."""
    return all(abs(pixel[i] - target_color[i]) <= tolerance for i in range(3))
def putOrangeDots():
    base_json = {
        "depth_factor": 4.0,
        "depth_path": "media\\image\\decoration\\arbre\\tree_mid_leaves_1depth.png",
        "emission_path": "",
        "flip_x": False,
        "flip_y": False,
        "key_color": {
            "a": 0,
            "b": 0,
            "g": 0,
            "r": 0
        },
        "name": "tree_mid_leaves_1_1",
        "path": "media\\image\\decoration\\arbre\\tree_mid_leaves_1.png",
        "position": {
            "x": 0.0,
            "y": 0.0,
            "z": -209.0
        },
        "rotation": 0.0,
        "scale": 0.5
    }

    # Load the image
    image = Image.open('colored_heightmapwith_farm.png').convert("RGB")
    pixels = image.load()

    # Define colors where black dots will be inserted with a tolerance
    target_colors = [(34, 139, 24), (34, 100, 34)]
    tolerance = 10  # Adjust this tolerance as needed

    # Define the number of black dots you want to insert
    num_dots = 650  # Adjust this to your preference

    # Get the dimensions of the image
    width, height = image.size

    # Collect all pixel coordinates that match the target colors within the tolerance
    target_coords = []
    for x in range(width):
        for y in range(height):
            for color in target_colors:
                if is_color_close(pixels[x, y], color, tolerance):
                    target_coords.append((x, y))
                    break  # No need to check other colors if a match is found

    # Insert black dots only on matching pixels and save their positions
    positions = []  # List to store (x, 0.0, z) positions
    for _ in range(min(num_dots, len(target_coords))):  # Ensure we don't exceed available coordinates
        x, z = random.choice(target_coords)
        pixels[x, z] = (0, 0, 0)  # Black color in RGB
        positions.append((x, 0.0, z))  # Save position with y = 0.0



    # Save or display the image
    image.save("image_with_black_dots.png")
    image.show()

    json_objects = []
    randomArbre = ['tree_mid_leaves_1','tree_mid_leaves_2','tree_mid_leaves_3','tree_mid_leaves_4','tree_mid_leaves_5']
    for i, pos in enumerate(positions, start=1):
        obj = base_json.copy()  # Copy the base JSON to modify
        obj["position"]["x"] = pos[0]  # Use index 0 for x
        obj["position"]["z"] = pos[2]
        # Choose a random tree from randomArbre
        random_tree = random.choice(randomArbre)
        obj["path"] = f"media\\image\\decoration\\arbre\\{random_tree}.png"
        # Also set the depth_path based on the random tree chosen
        obj["depth_path"] = f"media\\image\\decoration\\arbre\\{random_tree}depth.png"
        # Set the name based on the random tree
        obj["name"] = f"tree_mid_leaves_{i}"   # Remove the ".png" for name
        json_objects.append(obj)

    # Convert to JSON and print
    json_output = json.dumps(json_objects, indent=4)
    print(json_output)


def extract_split_and_denoise():
    im = Image.open('C:\\Users\\Yulia\\PycharmProjects\\creerImage\\image_with_black_dots.png')
    width, height = im.size

    # Calculate the dimensions for each of the 16 parts
    part_width = width // 4
    part_height = height // 4

    # Loop over the 4x4 grid to crop each piece
    pieces = []
    for row in range(4):
        for col in range(4):
            left = col * part_width
            upper = row * part_height
            right = left + part_width
            lower = upper + part_height
            piece = im.crop((left, upper, right, lower))

            # Apply noise reduction filter
            denoised_piece = piece.filter(ImageFilter.MedianFilter(size=3))  # Median filter for noise reduction
            pieces.append(denoised_piece)

    # Resize each piece to 4096x4096 and save it
    for i, piece in enumerate(pieces, start=1):
        resized_piece = piece.resize((4096, 4096))  # Resize to 4096x4096
        filename = f"image_piece_{i}_4096x4096.png"
        resized_piece.save(filename)
        print(f"Saved {filename} with size: {resized_piece.size}")


if __name__ == '__main__':
    extract_split_and_denoise()
    #plot_heightmap_3d()
    #putOrangeDots()