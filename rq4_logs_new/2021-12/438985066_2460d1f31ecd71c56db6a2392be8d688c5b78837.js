let imageHasLoaded = false;

let imagesElement = null;

const checkImagesHasLoaded = (images) => {
  const allImagePromises = images.map((image) => {
    return new Promise((resolve) => {
      const imageObj = new Image();
      imageObj.onload = () => {
        resolve(imageObj);
      };

      imageObj.src = image;
    });
  });

  return Promise.all(allImagePromises);
};

const createBeltScroller = (container, images) => {
  checkImagesHasLoaded(images).then((resolvedImages) => {
    imageHasLoaded = true;
    const beltDOMItems = images.map((image, index) => {
      const element = document.createElement('div');
      element.classList.add('article-img-block');

      const elementImgDiv = document.createElement('div');
      elementImgDiv.classList.add('article-img');

      const elementLink = document.createElement('a');
      elementLink.href = image;

      const elementImage = document.createElement('img');
      elementImage.src = image;
      elementImage.alt = `Earth in Blender step ${image}`;

      elementLink.appendChild(elementImage);
      elementImgDiv.appendChild(elementLink);
      element.appendChild(elementImgDiv);
      return element;
    });

    imagesElement = beltDOMItems.map((element) => element);

    beltDOMItems.forEach((beltDOMItem) => {
      container.appendChild(beltDOMItem);
    });
  });
};

const container = document.querySelector('#container');

createBeltScroller(container, [
  'data/0_100_maptheclouds_all_f.png',
  'data/1_cases.png',
  'data/2_flower.png',
  'data/3_geospatial_rel_3e.png',
  'data/4_5_maptheclouds_logo.png',
  'data/4_5_maptheclouds_logo_sm.png',
  'data/4_datavizro.png',
  'data/4_logo_datavizro_2.png',
  'data/4_logo_datavizro.png',
  'data/5_11_dist_circular.png',
  'data/5_13_rel_correlation.png',
  'data/5_14_rel_space.png',
  'data/5_15_rel_multivariate.png',
  'data/5_16_rel_trees.png',
  'data/5_18_rel_connections.png',
  'data/5_1_comp_parttowhole.png',
  'data/5_2_comp_pictogram.png',
  'data/5_3_comp_historical_img.png',
  'data/5_4_comp_magic.png',
  'data/5_5_comp_slope.png',
  'data/5_6_comp_exp.png',
  'data/5_7_dist_physical.png',
  'data/5_chartchall.png',
  'data/7a_mapstuff_logo.png',
  'data/7_mapstuff.png',
  'data/8_we_travel_logo.png',
  'data/8_we_travel_page2.png',
  'data/8_we_travel_page.png',
  'data/10_0_mapchall.png',

  'data/10_10_raster_meta.png',
  'data/10_11_3d_meta.png',
  'data/10_12_population_meta.png',
  'data/10_13_naturalearth_meta.png',
  'data/10_14_newtool_meta.png',
  'data/10_15_withoutcomp_meta.png',
  'data/10_16_urbanrural_meta.png',
  'data/10_17_land_meta.png',
  'data/10_18_water_meta.png',
  'data/10_19_islands_meta.png',
  'data/101_blender-earth.png',
  'data/10_1_points_meta.png',
  'data/10_20_movement_meta.png',
  'data/10_21_elevation_meta.png',
  'data/10_22_boundaries_meta.png',
  'data/10_23_ghsl_meta.png',
  'data/10_24_historical_meta.png',
  'data/10_25_interactive_meta.png',
  'data/10_26_choropleth_meta.png',
  'data/10_27_heatmap_meta.png',
  'data/10_28_earthnotflat_meta.png',
  'data/10_29_null_meta.png',
  'data/10_2_lines_meta.png',
  'data/102_snow_0.png',
  'data/10_30_metamapping_meta.png',
  'data/10_3_polygons_meta.png',
  'data/10_4_hexagons_meta.png',
  'data/10_5_openstreetmap_meta.png',
  'data/10_6_red_meta.png',
  'data/10_7_green_meta.png',
  'data/10_8_blue_meta.png',
  'data/10_9_monochrome_meta.png',
  'data/100_maptheclouds_all_f.png',
  'data/100_maptheclouds_all.png',
]);