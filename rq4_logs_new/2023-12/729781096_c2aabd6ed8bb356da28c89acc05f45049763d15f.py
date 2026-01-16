"""
Create Pages

Module that contains a function to initialize needed classes
for the main program.
"""

from typing import List
import pygame
from modules.interface_objects import Button, InputButton, Page


def create_pages() -> List[Page]:
    """Returns a list of Page objects for the system.

    First gets images, then creates buttons, then creates pages.
    """
    pygame.init()

    small_font = pygame.font.SysFont('arial', 20)
    large_font = pygame.font.SysFont('arial', 40)

    # Images
    canada_map_img = pygame.transform.scale(pygame.image.load('images/canada_map.jpg'),
                                            (960, 720))
    grass_img = pygame.transform.scale(pygame.image.load('images/grass.jpeg'),
                                       (960, 720))
    sky_img = pygame.transform.scale(pygame.image.load('images/sky.jpg'),
                                     (960, 720))

    # Button objects
    back = Button('normal', 'BACK', large_font)
    back.rect.topleft = (10, 10)
    all_regions = create_region_buttons(small_font)
    all_birds = create_bird_buttons(small_font, 180, 60, 200)
    all_ghgs = create_ghg_buttons(small_font, large_font, 180, 60, 200)
    page3_buttons = create_page3_buttons(small_font, large_font, 960, 720, 180)
    page4_buttons = create_page4_buttons(small_font, large_font, 960, 720, 180)

    return [Page(canada_map_img, all_regions),
            Page(grass_img, all_birds + [back]),
            Page(sky_img, all_ghgs + [back]),
            Page(sky_img, page3_buttons + [back]),
            Page(sky_img, page4_buttons + [back])
            ]


def create_region_buttons(font: pygame.font.Font) -> List[Button]:
    """Returns a list of Button objects that represent regions."""
    region_names = ['Alberta', 'British Columbia', 'Manitoba', 'New Brunswick',
                    'Newfoundland and Labrador', 'Northwest Territories', 'Nova Scotia', 'Nunavut',
                    'Ontario', 'Prince Edward Island', 'Quebec', 'Saskatchewan', 'Yukon', 'Canada']
    region_coords = [(254, 487), (129, 463), (425, 510), (790, 561), (793, 411), (265, 343),
                     (867, 609), (463, 254), (532, 561), (817, 520), (678, 506), (335, 550),
                     (124, 291), (50, 20)]

    buttons = [Button('normal', region, font) for region in region_names]

    for i in range(len(buttons)):
        buttons[i].rect.center = region_coords[i]

    return buttons


def create_bird_buttons(font: pygame.font.Font, x_margin: int, y_margin: int,
                        grid_box_size: int) -> List[Button]:
    """Returns a list of Button objects that represent birds."""
    bird_names = ['Waterfowl', 'Birds of Prey', 'Wetland Birds', 'Seabirds', 'Forest Birds',
                  'Shorebirds', 'Grassland Birds', 'Aerial Insectivores', 'All Other Birds']
    bird_images = create_bird_images()

    buttons = [Button('normal', bird_names[j], font, bird_images[j])
               for j in range(len(bird_names))]

    for i in range(len(buttons)):
        buttons[i].rect.center = \
            ((x_margin + grid_box_size * (i % 3) + grid_box_size // 2),
             (y_margin + grid_box_size * (i // 3) + grid_box_size // 2))

    return buttons


def create_ghg_buttons(small_font: pygame.font.Font, large_font: pygame.font.Font,
                       x_margin: int, y_margin: int, grid_box_size: int) -> List[Button]:
    """Returns a list of Button objects that represent greenhouse gases."""
    total = Button('normal', 'Total', large_font)
    multiple_regression = Button('normal', 'Multiple Regression', small_font)

    ghg_names = ['CO2', 'CH4', 'N2O', 'HFC', 'PFC', 'SF6', 'NF3']
    buttons = [Button('normal', ghg, large_font) for ghg in ghg_names] + \
              [total, multiple_regression]

    for i in range(len(buttons)):
        buttons[i].rect.center = \
            ((x_margin + grid_box_size * (i % 3) + grid_box_size // 2),
             (y_margin + grid_box_size * (i // 3) + grid_box_size // 2))

    return buttons


def create_page3_buttons(small_font: pygame.font.Font, large_font: pygame.font.Font,
                         window_width: int, window_height: int, x_margin: int) -> List[Button]:
    """Returns a list of Button objects that should appear on page 3.

    There is a button for inputting an amount of gas, a button for showing a graph, and
    a button for showing the estimated bird population change.
    """
    bird_output = Button('output', 'Bird Population Change (From 1970): 0 %', large_font)
    ghg_input = InputButton('input', 'Amount of Gas (kt): ', large_font, bird_output)
    show_graph = Button('normal', 'Show Graph', small_font)

    ghg_input.rect.topleft = (x_margin, window_height * 0.6)
    bird_output.rect.topleft = (x_margin, window_height * 0.4)
    show_graph.rect.center = (window_width // 2, window_height * 0.8)

    return [ghg_input, bird_output, show_graph]


def create_page4_buttons(small_font: pygame.font.Font, large_font: pygame.font.Font,
                         window_width: int, window_height: int, x_margin: int) -> List[Button]:
    """Returns a list of Button objects that should appear on page 4.

    There is a button for showing the estimated bird population change, several buttons for
    inputting an amount of gas, and several buttons for showing multiple regression coefficients
    for several gases.
    """
    ghg_names = ['CO2', 'CH4', 'N2O', 'HFC', 'PFC', 'SF6', 'NF3']

    multiple_regression_output = Button('output', 'Bird Population Change (From 1970): 0 %',
                                        large_font)

    all_ghg_input = [InputButton('input', f'{ghg} (kt): ', small_font, multiple_regression_output)
                     for ghg in ghg_names]
    all_ghg_coef = [Button('display', f'{ghg} Weight: ', small_font) for ghg in ghg_names]

    for i in range(len(all_ghg_input)):
        all_ghg_input[i].rect.topleft = (x_margin, window_height * 0.4 + i * 30)
        all_ghg_coef[i].rect.topleft = (window_width // 2 + x_margin,
                                        window_height * 0.4 + i * 30)

    multiple_regression_output.rect.topleft = (x_margin, window_height * 0.3)

    return all_ghg_coef + all_ghg_input + [multiple_regression_output]


def create_bird_images() -> List[pygame.Surface]:
    """Returns a list of bird images."""
    image_files = ['waterfowl.jpg', 'birds_of_prey.jpg', 'wetland_birds.jpg', 'seabirds.jpg',
                   'forest_birds.jpg', 'shorebirds.jpg', 'grassland_birds.jpg',
                   'aerial_insectivores.jpg', 'all_other_birds.png']
    return [pygame.transform.scale(pygame.image.load(f'images/{image_file}'), (200, 200))
            for image_file in image_files]


if __name__ == '__main__':
    # import python_ta

    # python_ta.check_all(config={
    #     'max-line-length': 100,
    #     'extra-imports': ['python_ta.contracts', 'dataclasses', 'datetime'],
    #     'disable': ['R1705', 'C0200'],
    # })

    # import python_ta.contracts

    # python_ta.contracts.DEBUG_CONTRACTS = False
    # python_ta.contracts.check_all_contracts()

    import doctest

    doctest.testmod()
