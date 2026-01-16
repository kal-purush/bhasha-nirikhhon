import matplotlib.pyplot as plt
import numpy as np
import streamlit as st
from PIL import Image

def create_kwikset_plot(pin_count=5, pins=14315, save_path="line_graph.png", width=0.95, height=0.35, dpi=300):
    inch_to_mm = 25.4
    # Kwikset pin depths
    depths = {
        0: .335,
        1: .329,
        2: .306,
        3: .283,
        4: .260,
        5: .237,
        6: .214,
        7: .191
    }

    # Kwikset pin spacing
    key_root_1 = 0
    key_root_2 = .125
    pin_1_start = .205
    pin_1_end = .289
    pin_1_spacer = pin_1_end + .01414
    pin_2_start = .355
    pin_2_end = .439
    pin_2_spacer = pin_2_end + .01414
    pin_3_start = .505
    pin_3_end = .589
    pin_3_spacer = pin_3_end + .01414
    pin_4_start = .655
    pin_4_end = .739
    pin_4_spacer = pin_4_end + .01414
    pin_5_start = .805
    pin_5_end = .889
    pin_5_spacer = pin_5_end + .01414
    pin_6_start = .955
    pin_6_end = 1.039
    key_taper_1 = 1.144
    key_taper_2 = 1.2265

    # Set key length
    if pin_count == 5:
        key_taper_1 = 1
        key_taper_2 = 1.17
    display_conversion = (3 * pin_count)
    kwikset_pin_only = [pin_1_start, pin_1_end, pin_1_spacer, pin_2_start, pin_2_end, pin_2_spacer, pin_3_start, pin_3_end, pin_3_spacer, pin_4_start, pin_4_end, pin_4_spacer, pin_5_start, pin_5_end, pin_5_spacer, pin_6_start, pin_6_end]
    Kiwkset_pin_display_spacing = [key_root_1, key_root_2, *kwikset_pin_only[0:display_conversion], key_taper_1, key_taper_2]

    # Convert pins integer to list of depths
    pin_depths = [depths[int(digit)] for digit in str(pins)]

    # Ensure the pin_depths list matches the pin_count
    if len(pin_depths) != pin_count:
        raise ValueError("The number of pins does not match the pin count")

    # Set pin values
    key_root_1_depth = depths[0]
    key_root_2_depth = depths[0]
    key_taper_depth = depths[1]
    key_end = depths[7]

    # Data for the line graph
    y = [key_root_1_depth, key_root_2_depth]
    for depth in pin_depths:
        y.extend([depth, depth, depth + 0.009 * np.sqrt(2)])
    y.extend([key_taper_depth, key_end])

    # Create the line graph
    darker_gold = '#b8860b'  # Darker gold color
    fig, ax = plt.subplots(figsize=(width, height))

    # Load and resize the image
    img = Image.open("kwikblank.png")
    img = img.resize((img.width // 2, img.height // 2), Image.ANTIALIAS)
    ax.imshow(img, extent=[0, 1.17, 0, 0.37], aspect='auto', zorder=-1)

    ax.plot(Kiwkset_pin_display_spacing, y, color=darker_gold)  # Plot the line graph with markers and gold color
    y = np.array(y)  # Convert y to a NumPy array
    ax.fill_between(Kiwkset_pin_display_spacing, y, y2=max(y), where=(y < max(y)), color='white')
    ax.set_xticks([])
    ax.set_yticks([])
    # Remove the border (box)
    for spine in ax.spines.values():
        spine.set_visible(False)
    # Set the vertical scale
    ax.set_ylim(0, 0.5)
    ax.set_xlim(0, 1.17)

    # Set the fixed ratio for spacing and height
    ax.set_aspect(aspect='auto', adjustable='box')

    # Save the figure as a PNG
    fig.savefig(save_path, dpi=dpi, bbox_inches='tight')

    return save_path

# Example usage
if __name__ == "__main__":
    st.image(create_kwikset_plot(pin_count=5, pins=14515))