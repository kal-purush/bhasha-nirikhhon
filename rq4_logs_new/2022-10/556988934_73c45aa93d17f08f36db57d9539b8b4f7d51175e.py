#!/home/poker/miniconda3/bin/python

import netCDF4
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import os
#import os.rename
#import os.remove
import shutil
import sys
from scipy import interpolate
from PIL import Image

aoslogo = Image.open('/home/poker/uw-aoslogo.png')
aoslogoheight = aoslogo.size[1]
aoslogowidth = aoslogo.size[0]

# We need a float array between 0-1, rather than
# a uint8 array between 0-255
aoslogo = np.array(aoslogo).astype(np.float) / 255

#def interpolate_red_channel(red_ds, resampled_ds):
#    """
#    Interpolate the red data channel to the same grid as another channel.
#    """
#    x_new = resampled_ds.variables['x'][:]
#    y_new = resampled_ds.variables['y'][::-1]
#
#    f = interpolate.interp2d(red_ds.variables['x'][:], red_ds.variables['y'][::-1],
#                             red_ds.variables['Sectorized_CMI'][::-1,], fill_value=0)
#    red_interpolated = f(x_new, y_new[::-1])
#    return x_new, y_new, red_interpolated



filechar=['AA','AB','AC','AD','AE','AF','AG','AH','AI','AJ','AK','AL','AM',
          'AN','AO','AP','AQ','AR','AS','AT','AU','AV','AW','AX','AY','AZ',
          'BA','BB','BC','BD','BE','BF','BG','BH','BI','BJ','BK','BL','BM',
          'BN','BO','BP','BQ','BR','BS','BT','BU','BV','BW','BX','BY','BZ',
          'CA','CB','CC','CD','CE','CF','CG','CH','CI','CJ','CK','CL','CM',
          'CN','CO','CP','CQ','CR','CS','CT','CU','CV','CW','CX','CY','CZ',
          'DA','DB','DC','DD','DE','DF','DG','DH','DI','DJ','DK','DL','DM',
          'DN','DO','DP','DQ','DR','DS','DT','DU','DV','DW','DX','DY','DZ',
          'EA','EB','EC','ED','EE','EF','EG','EH','EI','EJ','EK','EL','EM',
          'EN','EO','EP','EQ','ER','ES','ET','EU','EV','EW','EX','EY','EZ']

#print(filechar[1])

prod_id = "TIRC"
#dt="201703051957"
dt = sys.argv[1]

# Start red band
band="02"
#f = netCDF4.Dataset("/weather/data/goes16/TIRC/"+band+"/"+dt+"_PAA.nc")
red_ds = netCDF4.Dataset("/weather/data/goes16/"+prod_id+"/"+band+"/"+dt+"_PAA.nc")
red_data = np.zeros(shape=(red_ds.product_rows,red_ds.product_columns))
red_xa= np.zeros(shape=(red_ds.product_columns))
red_ya= np.zeros(shape=(red_ds.product_rows))


print("RED DS")
#print(red_ds)

data_var = red_ds.variables['Sectorized_CMI']
red_data[0:red_ds.product_tile_height,0:red_ds.product_tile_width] = data_var[:]
#data_var2 = g.variables['Sectorized_CMI']

#print(data_var)

x = red_ds.variables['x'][:]
y = red_ds.variables['y'][:]
red_xa[red_ds.tile_column_offset:red_ds.tile_column_offset+red_ds.product_tile_width] = x[:]
red_ya[red_ds.tile_row_offset:red_ds.tile_row_offset+red_ds.product_tile_height] = y[:]

if red_ds.number_product_tiles > 1:
# this goes from 1 to number of tiles - 1
    for i in range(1,red_ds.number_product_tiles):
#    print(filechar[i])
        if os.path.isfile("/weather/data/goes16/TIRC/"+band+"/"+dt+"_P"+filechar[i]+".nc"):
            g = netCDF4.Dataset("/weather/data/goes16/TIRC/"+band+"/"+dt+"_P"+filechar[i]+".nc")
#        print(g)
            data_var2 = g.variables['Sectorized_CMI']
            red_data[g.tile_row_offset:g.tile_row_offset+g.product_tile_height,g.tile_column_offset:g.tile_column_offset+g.product_tile_width]=data_var2[:]
            x = g.variables['x'][:]
            y = g.variables['y'][:]
            red_xa[g.tile_column_offset:g.tile_column_offset+g.product_tile_width] = x[:]
            red_ya[g.tile_row_offset:g.tile_row_offset+g.product_tile_height] = y[:]
            g.close


##a[g.tile_column_offset:g.tile_column_offset+g.product_tile_width,g.tile_row_offset:g.tile_row_offset+g.product_tile_height]=data_var[:]
#    a[g.tile_row_offset:g.tile_row_offset+g.product_tile_height,g.tile_column_offset:g.tile_column_offset+g.product_tile_width]=data_var2[:]
##print(a)

# swap zeros for ones
red_data[red_data==0.] = 1.

# Start blue band
band="01"
#f = netCDF4.Dataset("/weather/data/goes16/TIRC/"+band+"/"+dt+"_PAA.nc")
blue_ds = netCDF4.Dataset("/weather/data/goes16/"+prod_id+"/"+band+"/"+dt+"_PAA.nc")
blue_data = np.zeros(shape=(blue_ds.product_rows,blue_ds.product_columns))
blue_xa= np.zeros(shape=(blue_ds.product_columns))
blue_ya= np.zeros(shape=(blue_ds.product_rows))


print("BLUE DS")
#print(blue_ds)

blue_data_var = blue_ds.variables['Sectorized_CMI']
blue_data[0:blue_ds.product_tile_height,0:blue_ds.product_tile_width] = blue_data_var[:]
#data_var2 = g.variables['Sectorized_CMI']

#print(blue_data_var)

x = blue_ds.variables['x'][:]
y = blue_ds.variables['y'][:]
blue_xa[blue_ds.tile_column_offset:blue_ds.tile_column_offset+blue_ds.product_tile_width] = x[:]
blue_ya[blue_ds.tile_row_offset:blue_ds.tile_row_offset+blue_ds.product_tile_height] = y[:]

if blue_ds.number_product_tiles > 1:
# this goes from 1 to number of tiles - 1
    for i in range(1,blue_ds.number_product_tiles):
#    print(filechar[i])
        if os.path.isfile("/weather/data/goes16/TIRC/"+band+"/"+dt+"_P"+filechar[i]+".nc"):
            g = netCDF4.Dataset("/weather/data/goes16/TIRC/"+band+"/"+dt+"_P"+filechar[i]+".nc")
#        print(g)
            data_var2 = g.variables['Sectorized_CMI']
            blue_data[g.tile_row_offset:g.tile_row_offset+g.product_tile_height,g.tile_column_offset:g.tile_column_offset+g.product_tile_width]=data_var2[:]
            x = g.variables['x'][:]
            y = g.variables['y'][:]
            blue_xa[g.tile_column_offset:g.tile_column_offset+g.product_tile_width] = x[:]
            blue_ya[g.tile_row_offset:g.tile_row_offset+g.product_tile_height] = y[:]
            g.close


##a[g.tile_column_offset:g.tile_column_offset+g.product_tile_width,g.tile_row_offset:g.tile_row_offset+g.product_tile_height]=data_var[:]
#    a[g.tile_row_offset:g.tile_row_offset+g.product_tile_height,g.tile_column_offset:g.tile_column_offset+g.product_tile_width]=data_var2[:]
##print(a)

# swap zeros for ones
blue_data[blue_data==0.] = 1.

# Start veggie band
band="03"
#f = netCDF4.Dataset("/weather/data/goes16/TIRC/"+band+"/"+dt+"_PAA.nc")
veggie_ds = netCDF4.Dataset("/weather/data/goes16/"+prod_id+"/"+band+"/"+dt+"_PAA.nc")
veggie_data = np.zeros(shape=(veggie_ds.product_rows,veggie_ds.product_columns))
veggie_xa= np.zeros(shape=(veggie_ds.product_columns))
veggie_ya= np.zeros(shape=(veggie_ds.product_rows))


print("VEGGIE DS")
#print(veggie_ds)

data_var = veggie_ds.variables['Sectorized_CMI']
veggie_data[0:veggie_ds.product_tile_height,0:veggie_ds.product_tile_width] = data_var[:]
#data_var2 = g.variables['Sectorized_CMI']

#print(data_var)

x = veggie_ds.variables['x'][:]
y = veggie_ds.variables['y'][:]
veggie_xa[veggie_ds.tile_column_offset:veggie_ds.tile_column_offset+veggie_ds.product_tile_width] = x[:]
veggie_ya[veggie_ds.tile_row_offset:veggie_ds.tile_row_offset+veggie_ds.product_tile_height] = y[:]

if red_ds.number_product_tiles > 1:
# this goes from 1 to number of tiles - 1
    for i in range(1,veggie_ds.number_product_tiles):
#    print(filechar[i])
        if os.path.isfile("/weather/data/goes16/TIRC/"+band+"/"+dt+"_P"+filechar[i]+".nc"):
            g = netCDF4.Dataset("/weather/data/goes16/TIRC/"+band+"/"+dt+"_P"+filechar[i]+".nc")
#        print(g)
            data_var2 = g.variables['Sectorized_CMI']
            veggie_data[g.tile_row_offset:g.tile_row_offset+g.product_tile_height,g.tile_column_offset:g.tile_column_offset+g.product_tile_width]=data_var2[:]
            x = g.variables['x'][:]
            y = g.variables['y'][:]
            veggie_xa[g.tile_column_offset:g.tile_column_offset+g.product_tile_width] = x[:]
            veggie_ya[g.tile_row_offset:g.tile_row_offset+g.product_tile_height] = y[:]
            g.close


#a[g.tile_column_offset:g.tile_column_offset+g.product_tile_width,g.tile_row_offset:g.tile_row_offset+g.product_tile_height]=data_var[:]
#    a[g.tile_row_offset:g.tile_row_offset+g.product_tile_height,g.tile_column_offset:g.tile_column_offset+g.product_tile_width]=data_var2[:]
#print(a)

# swap zeros for ones
veggie_data[veggie_data==0.] = 1.





print("Blue Data average")
print(np.average(blue_data))
if np.average(blue_data) > 0.7:
    quit()


proj_var = blue_ds.variables[blue_data_var.grid_mapping]

import cartopy.crs as ccrs

# Create a Globe specifying a spherical earth with the correct radius
globe = ccrs.Globe(ellipse='sphere', semimajor_axis=proj_var.semi_major,
                   semiminor_axis=proj_var.semi_minor)

proj = ccrs.LambertConformal(central_longitude=proj_var.longitude_of_central_meridian,
                             central_latitude=proj_var.latitude_of_projection_origin,
                             standard_parallels=[proj_var.standard_parallel],
                             globe=globe)


image_rows=blue_ds.product_rows
image_columns=blue_ds.product_columns
wi_image_crop_top=696
wi_image_crop_bottom=-3516
wi_image_crop_left=2580
wi_image_crop_right=-2440

wi_image_size_y=(image_rows+wi_image_crop_bottom-wi_image_crop_top)
wi_image_size_x=(image_columns+wi_image_crop_right-wi_image_crop_left)

#print("wi image size")
#print(wi_image_size_x, wi_image_size_y)

#wi_image_size_x=float(wi_image_size_x)/120.
#wi_image_size_y=float(wi_image_size_y)/120.
wi_image_size_x=float(wi_image_size_x)/80.
wi_image_size_y=float(wi_image_size_y)/80.

mw_image_crop_top=630
mw_image_crop_bottom=-2882
mw_image_crop_left=1750
mw_image_crop_right=-2156

mw_image_size_y=(image_rows+mw_image_crop_bottom-mw_image_crop_top)
mw_image_size_x=(image_columns+mw_image_crop_right-mw_image_crop_left)

#print("mw image size")
#print(mw_image_size_x, mw_image_size_y)

mw_image_size_x=float(mw_image_size_x)/150.
mw_image_size_y=float(mw_image_size_y)/150.

conus_image_crop_top=260
conus_image_crop_bottom=-1352
conus_image_crop_left=100
conus_image_crop_right=-450

conus_image_size_y=(image_rows+conus_image_crop_bottom-conus_image_crop_top)
conus_image_size_x=(image_columns+conus_image_crop_right-conus_image_crop_left)

#print("conus image size")
#print(conus_image_size_x, conus_image_size_y)

conus_image_size_x=float(conus_image_size_x)/300.
conus_image_size_y=float(conus_image_size_y)/300.

# Northeast sector
ne_image_crop_top=0
ne_image_crop_bottom=-4408
ne_image_crop_left=5900
ne_image_crop_right=-300

ne_image_size_y=(image_rows+ne_image_crop_bottom-ne_image_crop_top)
ne_image_size_x=(image_columns+ne_image_crop_right-ne_image_crop_left)

ne_image_size_x=float(ne_image_size_x)/400.
ne_image_size_y=float(ne_image_size_y)/400.



# Fullres Southern WI sector

swi_image_crop_top=1900
swi_image_crop_bottom=-6400
swi_image_crop_left=5860
swi_image_crop_right=-5280

swi_image_size_y=(image_rows+swi_image_crop_bottom-swi_image_crop_top)
swi_image_size_x=(image_columns+swi_image_crop_right-swi_image_crop_left)

swi_image_size_x=float(swi_image_size_x)/65.
swi_image_size_y=float(swi_image_size_y)/65.

# Fullres Colorado sector

co_image_crop_top=2600
co_image_crop_bottom=-5350
co_image_crop_left=2900
co_image_crop_right=-7950

co_image_size_y=(image_rows+co_image_crop_bottom-co_image_crop_top)
co_image_size_x=(image_columns+co_image_crop_right-co_image_crop_left)

co_image_size_x=float(co_image_size_x)/65.
co_image_size_y=float(co_image_size_y)/65.

# Fullres Florida sector

fl_image_crop_top=5000
fl_image_crop_bottom=-2800
fl_image_crop_left=6860
fl_image_crop_right=-3680

fl_image_size_y=(image_rows+fl_image_crop_bottom-fl_image_crop_top)
fl_image_size_x=(image_columns+fl_image_crop_right-fl_image_crop_left)

fl_image_size_x=float(fl_image_size_x)/65.
fl_image_size_y=float(fl_image_size_y)/65.

# Gulf of Mexico sector

gulf_image_crop_top=2146
gulf_image_crop_bottom=-620
gulf_image_crop_left=2348
gulf_image_crop_right=-452

gulf_image_size_y=(image_rows+gulf_image_crop_bottom-gulf_image_crop_top)
gulf_image_size_x=(image_columns+gulf_image_crop_right-gulf_image_crop_left)

gulf_image_size_x=float(gulf_image_size_x)/120.
gulf_image_size_y=float(gulf_image_size_y)/120.

# Alex special sector
alex_image_crop_top=2800
alex_image_crop_bottom=-501
alex_image_crop_left=400
alex_image_crop_right=-3670

alex_image_size_y=(image_rows+alex_image_crop_bottom-alex_image_crop_top)
alex_image_size_x=(image_columns+alex_image_crop_right-alex_image_crop_left)

alex_image_size_x=float(alex_image_size_x)/130.
alex_image_size_y=float(alex_image_size_y)/130.


# Create a new figure with size 10" by 10"
# WI crop
fig = plt.figure(figsize=(wi_image_size_x,wi_image_size_y),dpi=80.)
# Midwest crop
fig2 = plt.figure(figsize=(mw_image_size_x,mw_image_size_y),dpi=160.)
# CONUS crop
fig3 = plt.figure(figsize=(conus_image_size_x,conus_image_size_y),dpi=160.)
## Northeast crop
#fig4 = plt.figure(figsize=(ne_image_size_x,ne_image_size_y),dpi=40.)
## Wisconsin fullres crop
#fig5 = plt.figure(figsize=(swi_image_size_x,swi_image_size_y),dpi=83.)
## Colorado fullres crop
#fig6 = plt.figure(figsize=(co_image_size_x,co_image_size_y),dpi=83.)
## Florida fullres crop
#fig7 = plt.figure(figsize=(fl_image_size_x,fl_image_size_y),dpi=83.)
# Gulf of Mexico region
fig8 = plt.figure(figsize=(gulf_image_size_x,gulf_image_size_y),dpi=83.)
# Full res
fig9 = plt.figure(figsize=(image_columns/80.,image_rows/80.))
## Alex stormchase crop
fig10 = plt.figure(figsize=(alex_image_size_x,alex_image_size_y),dpi=83.)

# Put a single axes on this figure; set the projection for the axes to be our
# Lambert conformal projection
ax = fig.add_subplot(1, 1, 1, projection=proj)
ax.outline_patch.set_edgecolor('none')
ax2 = fig2.add_subplot(1, 1, 1, projection=proj)
ax2.outline_patch.set_edgecolor('none')
ax3 = fig3.add_subplot(1, 1, 1, projection=proj)
ax3.outline_patch.set_edgecolor('none')
#ax4 = fig4.add_subplot(1, 1, 1, projection=proj)
#ax5 = fig5.add_subplot(1, 1, 1, projection=proj)
#ax6 = fig6.add_subplot(1, 1, 1, projection=proj)
#ax7 = fig7.add_subplot(1, 1, 1, projection=proj)
ax8 = fig8.add_subplot(1, 1, 1, projection=proj)
ax8.outline_patch.set_edgecolor('none')
ax9 = fig9.add_subplot(1, 1, 1, projection=proj)
ax9.outline_patch.set_edgecolor('none')
ax10 = fig10.add_subplot(1, 1, 1, projection=proj)
ax10.outline_patch.set_edgecolor('none')

import matplotlib as mpl


print("interpolate red")

x_new = blue_xa[:]
y_new = blue_ya[::-1]

fint = interpolate.interp2d(red_xa[:], red_ya[::-1],
                         red_data[::-1,], fill_value=1)
red_interpolated = fint(x_new, y_new[::-1])


# ## THIS IS THE NATURAL, SQRT ONE ## #
## Part one of Kaba's pseudo green
#
#print("calculate green")
green_data = (.1*veggie_data) + (.45*blue_data) + (.45*red_interpolated[::-1,:])
##green_data = (.2*veggie_data) + (.45*blue_data) + (.35*red_interpolated[::-1,:])
##green_data = (.15*veggie_data) + (.27*blue_data) + (.58*red_interpolated[::-1,:])
##green_data = (.15*veggie_data) + (.10*blue_data) + (.75*red_interpolated[::-1,:])
#
#print("calc sqrt of red")
#red_interpolated = np.sqrt(red_interpolated)
#print("calc sqrt of blue")
#blue_data = np.sqrt(blue_data)
#print("calc sqrt of green")
#green_data = np.sqrt(green_data)
#
## Kaba's second magic contrast part
#
## This may need to change when NOAAPORT files get fixed
#maxValue=1.0
#acont=0.1
#amax=1.0067
#amid=0.5
#afact=(amax*(acont+maxValue)/(maxValue*(amax-acont)))
## Red part
#
#print("Kaba part 2 red")
#
#red_interpolated = (afact*(red_interpolated-amid)+amid)
#red_interpolated[red_interpolated <= 0.0392] = 0
#red_interpolated[red_interpolated >=1.0] = 1.0
#
## Blue part
#
#print("Kaba part 2 blue")
#
#blue_data = (afact*(blue_data-amid)+amid)
#blue_data[blue_data <= 0.0392] = 0
#blue_data[blue_data >=1.0] = 1.0
#
## Green part
#print("Kaba part 2 green")
#
#green_data = (afact*(green_data-amid)+amid)
#green_data[green_data <= 0.0392] = 0
#green_data[green_data >=1.0] = 1.0
#

# ## THIS IS THE HISTOGRAM ENHANCED VERSION ## #

def histeq(im,nbr_bins=65536):
    """    Histogram equalization of a grayscale image. """

    # get image histogram
    imhist,bins = np.histogram(im.flatten(),nbr_bins,normed=False, range=(0.002,0.998))
    cdf = imhist.cumsum() # cumulative distribution function
    cdf = 65535 * cdf / cdf[-1] # normalize

    # use linear interpolation of cdf to find new pixel values
    im2 = np.interp(im.flatten(),bins[:-1],cdf)
    im2 = im2/65536.

    return im2.reshape(im.shape), cdf

red_interpolated_data2,rcdf = histeq(red_interpolated)
green_data2,gcdf = histeq(green_data)
blue_data2,bcdf = histeq(blue_data)

blue_data2 = 0.95*blue_data2


print("stack 3 colors")
rgb_data = np.dstack([red_interpolated_data2[::-1,:], green_data2, blue_data2])


# Plot the data using a simple greyscale colormap (with black for low values);
# set the colormap to extend over a range of values from 140 to 255.
# Note, we save the image returned by imshow for later...
#im = ax.imshow(data_var[:], extent=(x[0], x[-1], y[0], y[-1]), origin='upper',
#               cmap='Greys_r', norm=plt.Normalize(0, 256))
#im = ax.imshow(data_var[:], extent=(x[0], x[-1], y[0], y[-1]), origin='upper',
#im = ax.imshow(a[:], extent=(xa[0], xa[-1], ya[-1], ya[0]), origin='upper',
#               cmap='Greys_r')
#im = ax.imshow(a[250:-3000,2000:-2000], extent=(xa[2000],xa[-2000],ya[-3000],ya[250]), origin='upper',cmap='Greys_r')
#im = ax.imshow(a[250:-3500,2500:-2200], extent=(xa[2500],xa[-2200],ya[-3500],ya[250]), origin='upper',cmap='Greys_r')
#im = ax.imshow(data_var[:], extent=(x[0], x[-1], y[0], y[-1]), origin='upper')
#im = ax2.imshow(a[:], extent=(xa[1],xa[-1],ya[-1],ya[1]), origin='upper', cmap='Greys_r')

im = ax.imshow(rgb_data[wi_image_crop_top:wi_image_crop_bottom,wi_image_crop_left:wi_image_crop_right], extent=(blue_xa[wi_image_crop_left],blue_xa[wi_image_crop_right],blue_ya[wi_image_crop_bottom],blue_ya[wi_image_crop_top]), origin='upper')
im = ax2.imshow(rgb_data[mw_image_crop_top:mw_image_crop_bottom,mw_image_crop_left:mw_image_crop_right], extent=(blue_xa[mw_image_crop_left],blue_xa[mw_image_crop_right],blue_ya[mw_image_crop_bottom],blue_ya[mw_image_crop_top]), origin='upper')
im = ax3.imshow(rgb_data[conus_image_crop_top:conus_image_crop_bottom,conus_image_crop_left:conus_image_crop_right], extent=(blue_xa[conus_image_crop_left],blue_xa[conus_image_crop_right],blue_ya[conus_image_crop_bottom],blue_ya[conus_image_crop_top]), origin='upper')
#im = ax4.imshow(rgb_data[ne_image_crop_top:ne_image_crop_bottom,ne_image_crop_left:ne_image_crop_right], extent=(blue_xa[ne_image_crop_left],blue_xa[ne_image_crop_right],blue_ya[ne_image_crop_bottom],blue_ya[ne_image_crop_top]), origin='upper')
#im = ax5.imshow(rgb_data[swi_image_crop_top:swi_image_crop_bottom,swi_image_crop_left:swi_image_crop_right], extent=(blue_xa[swi_image_crop_left],blue_xa[swi_image_crop_right],blue_ya[swi_image_crop_bottom],blue_ya[swi_image_crop_top]), origin='upper')
#im = ax6.imshow(rgb_data[co_image_crop_top:co_image_crop_bottom,co_image_crop_left:co_image_crop_right], extent=(blue_xa[co_image_crop_left],blue_xa[co_image_crop_right],blue_ya[co_image_crop_bottom],blue_ya[co_image_crop_top]), origin='upper')
#im = ax7.imshow(rgb_data[fl_image_crop_top:fl_image_crop_bottom,fl_image_crop_left:fl_image_crop_right], extent=(blue_xa[fl_image_crop_left],blue_xa[fl_image_crop_right],blue_ya[fl_image_crop_bottom],blue_ya[fl_image_crop_top]), origin='upper')
im = ax8.imshow(rgb_data[gulf_image_crop_top:gulf_image_crop_bottom,gulf_image_crop_left:gulf_image_crop_right], extent=(blue_xa[gulf_image_crop_left],blue_xa[gulf_image_crop_right],blue_ya[gulf_image_crop_bottom],blue_ya[gulf_image_crop_top]), origin='upper')
im = ax9.imshow(rgb_data[:], extent=(blue_xa[0],blue_xa[-1],blue_ya[-1],blue_ya[0]), origin='upper')
im = ax10.imshow(rgb_data[alex_image_crop_top:alex_image_crop_bottom,alex_image_crop_left:alex_image_crop_right], extent=(blue_xa[alex_image_crop_left],blue_xa[alex_image_crop_right],blue_ya[alex_image_crop_bottom],blue_ya[alex_image_crop_top]), origin='upper')

import cartopy.feature as cfeat

ax.coastlines(resolution='50m', color='green')
ax2.coastlines(resolution='50m', color='green')
ax3.coastlines(resolution='50m', color='green')
#ax4.coastlines(resolution='50m', color='green')
#ax5.coastlines(resolution='50m', color='green')
#ax6.coastlines(resolution='50m', color='green')
#ax7.coastlines(resolution='50m', color='green')
ax8.coastlines(resolution='50m', color='green')
ax9.coastlines(resolution='50m', color='green')
ax10.coastlines(resolution='50m', color='green')

# Add country borders with a thick line.
ax.add_feature(cfeat.BORDERS, linewidth='1', edgecolor='green')
ax2.add_feature(cfeat.BORDERS, linewidth='1', edgecolor='green')
ax3.add_feature(cfeat.BORDERS, linewidth='1', edgecolor='green')
#ax4.add_feature(cfeat.BORDERS, linewidth='1', edgecolor='green')
#ax5.add_feature(cfeat.BORDERS, linewidth='1', edgecolor='green')
#ax6.add_feature(cfeat.BORDERS, linewidth='1', edgecolor='green')
#ax7.add_feature(cfeat.BORDERS, linewidth='1', edgecolor='green')
ax8.add_feature(cfeat.BORDERS, linewidth='1', edgecolor='green')
ax9.add_feature(cfeat.BORDERS, linewidth='1', edgecolor='green')
ax10.add_feature(cfeat.BORDERS, linewidth='1', edgecolor='green')

# Set up a feature for the state/province lines. Tell cartopy not to fill in the polygons
state_boundaries = cfeat.NaturalEarthFeature(category='cultural',
                                             name='admin_1_states_provinces_lakes',
                                             scale='50m', facecolor='none', edgecolor='red')

state_boundaries2 = cfeat.NaturalEarthFeature(category='cultural',
                                             name='admin_1_states_provinces_lakes',
                                             scale='10m', facecolor='none', edgecolor='red')

# Add the feature with dotted lines, denoted by ':'
ax.add_feature(state_boundaries, linestyle=':')
ax2.add_feature(state_boundaries, linestyle=':')
ax3.add_feature(state_boundaries, linestyle=':')
#ax4.add_feature(state_boundaries, linestyle=':')
#ax5.add_feature(state_boundaries2, linewidth=2)
#ax6.add_feature(state_boundaries2, linewidth=2)
#ax7.add_feature(state_boundaries2, linewidth=2)
ax8.add_feature(state_boundaries, linestyle=':')
ax9.add_feature(state_boundaries, linestyle=':')
ax10.add_feature(state_boundaries, linestyle=':')

from cartopy.io.shapereader import Reader
#fname = '/home/poker/resources/counties.shp'
fname = '/home/poker/resources/cb_2016_us_county_5m.shp'
counties = Reader(fname)

#ax5.add_geometries(counties.geometries(), ccrs.PlateCarree(), edgecolor='darkgreen', facecolor='None')
#ax6.add_geometries(counties.geometries(), ccrs.PlateCarree(), edgecolor='darkgreen', facecolor='None')
#ax7.add_geometries(counties.geometries(), ccrs.PlateCarree(), edgecolor='darkgreen', facecolor='None')
#
#ax5.plot(-89.4012, 43.0731, 'bo', markersize=3, transform=ccrs.Geodetic())
#ax5.text(-89.50, 43.02, 'MSN', transform=ccrs.Geodetic(), color='darkorange')
#ax5.plot(-87.9065, 43.0389, 'bo', markersize=3, transform=ccrs.Geodetic())
#ax5.text(-88.00, 42.98, 'MKE', transform=ccrs.Geodetic(), color='darkorange')
#ax5.plot(-91.2396, 43.8014, 'bo', markersize=3, transform=ccrs.Geodetic())
#ax5.text(-91.33, 43.75, 'LSE', transform=ccrs.Geodetic(), color='darkorange')
#ax5.plot(-88.0198, 44.5192, 'bo', markersize=3, transform=ccrs.Geodetic())
#ax5.text(-88.11, 44.46, 'GRB', transform=ccrs.Geodetic(), color='darkorange')
#
#ax6.plot(-104.9903, 39.7392, 'bo', markersize=3, transform=ccrs.Geodetic())
#ax6.text(-105.09, 39.68, 'DEN', transform=ccrs.Geodetic(), color='darkorange')
#ax6.plot(-105.2705, 40.0150, 'bo', markersize=3, transform=ccrs.Geodetic())
#ax6.text(-105.37, 39.96, 'BOU', transform=ccrs.Geodetic(), color='darkorange')
#ax6.plot(-105.0844, 40.5853, 'bo', markersize=3, transform=ccrs.Geodetic())
#ax6.text(-105.18, 40.53, 'FNL', transform=ccrs.Geodetic(), color='darkorange')
#ax6.plot(-108.5506, 39.0639, 'bo', markersize=3, transform=ccrs.Geodetic())
#ax6.text(-108.65, 39.01, 'GJT', transform=ccrs.Geodetic(), color='darkorange')
#ax6.plot(-104.8214, 38.8339, 'bo', markersize=3, transform=ccrs.Geodetic())
#ax6.text(-104.92, 38.78, 'COS', transform=ccrs.Geodetic(), color='darkorange')
#ax6.plot(-104.6091, 38.2544, 'bo', markersize=3, transform=ccrs.Geodetic())
#ax6.text(-104.70, 38.20, 'PUB', transform=ccrs.Geodetic(), color='darkorange')

import datetime

time_var = blue_ds.start_date_time

jyr = time_var[0:4]
jday = time_var[4:7]
#print(jday)

date = datetime.datetime(int(jyr), 1, 1) + datetime.timedelta(int(jday)-1)

time_string = 'GOES16 Enhanced color visible valid %s '%date.strftime('%Y %b %d')+time_var[7:9]+":"+time_var[9:11]+":"+time_var[11:13]+" GMT"
print(time_string)

from matplotlib import patheffects
outline_effect = [patheffects.withStroke(linewidth=2, foreground='black')]


#2017/065 20:04:00:30
text = ax.text(0.50, 0.97, time_string,
    horizontalalignment='center', transform = ax.transAxes,
    color='yellow', fontsize='large', weight='bold')

text.set_path_effects(outline_effect)

text2 = ax2.text(0.50, 0.97, time_string,
    horizontalalignment='center', transform = ax2.transAxes,
    color='yellow', fontsize='large', weight='bold')

text2.set_path_effects(outline_effect)

text3 = ax3.text(0.50, 0.95, time_string,
    horizontalalignment='center', transform = ax3.transAxes,
    color='darkorange', fontsize='large', weight='bold')

text3.set_path_effects(outline_effect)

#text = ax4.text(0.50, 0.95, time_string,
#    horizontalalignment='center', transform = ax4.transAxes,
#    color='darkorange', fontsize='large', weight='bold')
#
#text = ax5.text(0.50, 0.95, time_string,
#    horizontalalignment='center', transform = ax5.transAxes,
#    color='darkorange', fontsize='large', weight='bold')
#
#text = ax6.text(0.50, 0.95, time_string,
#    horizontalalignment='center', transform = ax6.transAxes,
#    color='darkorange', fontsize='large', weight='bold')
#
#text = ax7.text(0.50, 0.95, time_string,
#    horizontalalignment='center', transform = ax7.transAxes,
#    color='darkorange', fontsize='large', weight='bold')
#
text8 = ax8.text(0.50, 0.95, time_string,
    horizontalalignment='center', transform = ax8.transAxes,
    color='darkorange', fontsize='large', weight='bold')

text8.set_path_effects(outline_effect)

text9 = ax9.text(0.50, 0.97, time_string,
    horizontalalignment='center', transform = ax9.transAxes,
    color='yellow', fontsize='large', weight='bold')

text9.set_path_effects(outline_effect)

text10= ax10.text(0.50, 0.95, time_string,
    horizontalalignment='center', transform = ax10.transAxes,
    color='darkorange', fontsize='large', weight='bold')

text10.set_path_effects(outline_effect)

filename1="/whirlwind/goes16/vis_color_enh/wi/"+dt+"_wi.jpg"
filename2="/whirlwind/goes16/vis_color_enh/mw/"+dt+"_mw.jpg"
filename3="/whirlwind/goes16/vis_color_enh/conus/"+dt+"_conus.jpg"
#filename4="/whirlwind/goes16/vis_color/ne/"+dt+"_ne.jpg"
#filename5="/whirlwind/goes16/vis_color/swi/"+dt+"_swi.jpg"
#filename6="/whirlwind/goes16/vis_color/co/"+dt+"_co.jpg"
#filename7="/whirlwind/goes16/vis_color/fl/"+dt+"_fl.jpg"
filename8="/whirlwind/goes16/vis_color_enh/gulf/"+dt+"_gulf.jpg"
filename9="/whirlwind/goes16/vis_color_enh/full/"+dt+"_full.jpg"
filename10="/whirlwind/goes16/vis_color_enh/baja/"+dt+"_baja.jpg"

fig.figimage(aoslogo,  10, fig.bbox.ymax - aoslogoheight - 18  , zorder=10)
fig2.figimage(aoslogo,  10, int(fig2.bbox.ymax/2) - aoslogoheight - 18  , zorder=10)
fig3.figimage(aoslogo,  10, int(fig3.bbox.ymax/2) - aoslogoheight - 18  , zorder=10)
#fig4.figimage(aoslogo,  10, int(fig4.bbox.ymax/.5) - aoslogoheight - 18  , zorder=10)
#fig5.figimage(aoslogo,  10, int(fig5.bbox.ymax*.96386) - aoslogoheight - 18  , zorder=10)
#fig6.figimage(aoslogo,  10, int(fig6.bbox.ymax*.96386) - aoslogoheight - 18  , zorder=10)
#fig7.figimage(aoslogo,  10, int(fig7.bbox.ymax*.96386) - aoslogoheight - 18  , zorder=10)
#print("fig8.bbox.ymax/.96386",fig8.bbox.ymax/.96386)
fig8.figimage(aoslogo,  10, int(fig8.bbox.ymax*.96386) - aoslogoheight - 40  , zorder=10)
fig9.figimage(aoslogo,  10, fig9.bbox.ymax - aoslogoheight - 18  , zorder=10)
fig10.figimage(aoslogo,  10, int(fig10.bbox.ymax*.96386) - aoslogoheight - 25  , zorder=10)

fig.savefig(filename1, bbox_inches='tight')
fig2.savefig(filename2, bbox_inches='tight')
#fig2.savefig(filename2jpg, bbox_inches='tight')
fig3.savefig(filename3, bbox_inches='tight')
#fig4.savefig(filename4, bbox_inches='tight')
#fig5.savefig(filename5, bbox_inches='tight')
#fig6.savefig(filename6, bbox_inches='tight')
#fig7.savefig(filename7, bbox_inches='tight')
fig8.savefig(filename8, bbox_inches='tight')
fig9.savefig(filename9, bbox_inches='tight')
fig10.savefig(filename10, bbox_inches='tight')

#import os.rename    # os.rename(src,dest)
#import os.remove    # os.remove path
#import shutil.copy  # shutil.copy(src, dest)

#quit()

os.remove("/whirlwind/goes16/vis_color_enh/wi/latest_wi_72.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_71.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_72.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_70.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_71.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_69.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_70.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_68.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_69.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_67.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_68.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_66.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_67.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_65.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_66.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_64.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_65.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_63.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_64.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_62.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_63.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_61.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_62.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_60.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_61.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_59.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_60.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_58.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_59.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_57.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_58.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_56.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_57.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_55.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_56.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_54.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_55.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_53.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_54.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_52.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_53.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_51.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_52.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_50.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_51.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_49.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_50.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_48.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_49.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_47.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_48.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_46.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_47.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_45.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_46.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_44.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_45.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_43.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_44.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_42.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_43.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_41.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_42.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_40.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_41.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_39.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_40.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_38.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_39.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_37.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_38.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_36.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_37.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_35.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_36.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_34.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_35.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_33.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_34.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_32.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_33.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_31.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_32.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_30.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_31.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_29.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_30.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_28.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_29.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_27.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_28.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_26.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_27.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_25.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_26.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_24.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_25.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_23.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_24.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_22.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_23.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_21.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_22.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_20.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_21.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_19.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_20.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_18.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_19.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_17.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_18.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_16.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_17.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_15.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_16.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_14.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_15.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_13.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_14.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_12.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_13.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_11.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_12.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_10.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_11.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_9.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_10.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_8.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_9.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_7.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_8.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_6.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_7.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_5.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_6.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_4.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_5.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_3.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_4.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_2.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_3.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/wi/latest_wi_1.jpg", "/whirlwind/goes16/vis_color_enh/wi/latest_wi_2.jpg")

shutil.copy(filename1, "/whirlwind/goes16/vis_color_enh/wi/latest_wi_1.jpg")

# Midwest
os.remove("/whirlwind/goes16/vis_color_enh/mw/latest_mw_72.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_71.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_72.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_70.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_71.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_69.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_70.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_68.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_69.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_67.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_68.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_66.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_67.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_65.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_66.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_64.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_65.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_63.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_64.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_62.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_63.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_61.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_62.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_60.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_61.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_59.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_60.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_58.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_59.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_57.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_58.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_56.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_57.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_55.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_56.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_54.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_55.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_53.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_54.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_52.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_53.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_51.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_52.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_50.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_51.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_49.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_50.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_48.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_49.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_47.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_48.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_46.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_47.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_45.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_46.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_44.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_45.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_43.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_44.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_42.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_43.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_41.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_42.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_40.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_41.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_39.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_40.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_38.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_39.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_37.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_38.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_36.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_37.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_35.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_36.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_34.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_35.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_33.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_34.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_32.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_33.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_31.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_32.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_30.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_31.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_29.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_30.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_28.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_29.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_27.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_28.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_26.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_27.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_25.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_26.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_24.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_25.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_23.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_24.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_22.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_23.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_21.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_22.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_20.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_21.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_19.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_20.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_18.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_19.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_17.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_18.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_16.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_17.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_15.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_16.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_14.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_15.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_13.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_14.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_12.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_13.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_11.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_12.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_10.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_11.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_9.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_10.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_8.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_9.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_7.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_8.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_6.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_7.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_5.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_6.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_4.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_5.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_3.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_4.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_2.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_3.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/mw/latest_mw_1.jpg", "/whirlwind/goes16/vis_color_enh/mw/latest_mw_2.jpg")

shutil.copy(filename2, "/whirlwind/goes16/vis_color_enh/mw/latest_mw_1.jpg")


os.remove("/whirlwind/goes16/vis_color_enh/conus/latest_conus_72.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_71.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_72.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_70.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_71.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_69.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_70.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_68.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_69.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_67.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_68.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_66.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_67.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_65.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_66.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_64.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_65.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_63.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_64.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_62.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_63.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_61.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_62.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_60.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_61.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_59.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_60.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_58.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_59.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_57.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_58.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_56.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_57.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_55.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_56.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_54.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_55.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_53.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_54.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_52.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_53.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_51.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_52.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_50.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_51.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_49.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_50.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_48.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_49.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_47.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_48.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_46.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_47.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_45.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_46.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_44.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_45.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_43.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_44.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_42.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_43.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_41.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_42.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_40.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_41.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_39.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_40.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_38.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_39.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_37.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_38.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_36.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_37.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_35.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_36.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_34.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_35.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_33.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_34.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_32.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_33.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_31.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_32.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_30.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_31.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_29.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_30.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_28.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_29.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_27.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_28.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_26.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_27.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_25.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_26.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_24.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_25.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_23.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_24.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_22.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_23.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_21.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_22.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_20.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_21.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_19.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_20.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_18.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_19.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_17.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_18.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_16.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_17.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_15.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_16.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_14.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_15.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_13.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_14.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_12.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_13.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_11.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_12.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_10.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_11.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_9.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_10.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_8.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_9.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_7.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_8.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_6.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_7.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_5.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_6.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_4.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_5.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_3.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_4.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_2.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_3.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/conus/latest_conus_1.jpg", "/whirlwind/goes16/vis_color_enh/conus/latest_conus_2.jpg")

shutil.copy(filename3, "/whirlwind/goes16/vis_color_enh/conus/latest_conus_1.jpg")

# Gulf
os.remove("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_72.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_71.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_72.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_70.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_71.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_69.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_70.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_68.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_69.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_67.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_68.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_66.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_67.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_65.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_66.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_64.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_65.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_63.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_64.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_62.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_63.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_61.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_62.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_60.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_61.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_59.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_60.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_58.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_59.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_57.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_58.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_56.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_57.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_55.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_56.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_54.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_55.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_53.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_54.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_52.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_53.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_51.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_52.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_50.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_51.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_49.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_50.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_48.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_49.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_47.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_48.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_46.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_47.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_45.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_46.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_44.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_45.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_43.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_44.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_42.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_43.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_41.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_42.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_40.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_41.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_39.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_40.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_38.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_39.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_37.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_38.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_36.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_37.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_35.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_36.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_34.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_35.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_33.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_34.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_32.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_33.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_31.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_32.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_30.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_31.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_29.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_30.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_28.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_29.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_27.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_28.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_26.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_27.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_25.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_26.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_24.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_25.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_23.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_24.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_22.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_23.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_21.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_22.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_20.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_21.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_19.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_20.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_18.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_19.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_17.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_18.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_16.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_17.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_15.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_16.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_14.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_15.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_13.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_14.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_12.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_13.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_11.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_12.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_10.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_11.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_9.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_10.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_8.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_9.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_7.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_8.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_6.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_7.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_5.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_6.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_4.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_5.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_3.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_4.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_2.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_3.jpg")
os.rename("/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_1.jpg", "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_2.jpg")

shutil.copy(filename8, "/whirlwind/goes16/vis_color_enh/gulf/latest_gulf_1.jpg")

# Baja
os.remove("/whirlwind/goes16/vis_color/baja/latest_baja_72.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_71.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_72.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_70.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_71.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_69.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_70.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_68.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_69.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_67.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_68.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_66.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_67.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_65.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_66.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_64.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_65.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_63.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_64.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_62.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_63.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_61.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_62.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_60.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_61.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_59.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_60.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_58.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_59.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_57.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_58.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_56.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_57.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_55.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_56.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_54.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_55.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_53.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_54.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_52.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_53.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_51.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_52.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_50.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_51.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_49.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_50.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_48.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_49.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_47.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_48.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_46.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_47.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_45.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_46.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_44.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_45.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_43.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_44.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_42.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_43.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_41.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_42.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_40.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_41.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_39.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_40.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_38.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_39.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_37.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_38.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_36.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_37.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_35.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_36.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_34.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_35.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_33.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_34.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_32.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_33.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_31.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_32.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_30.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_31.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_29.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_30.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_28.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_29.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_27.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_28.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_26.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_27.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_25.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_26.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_24.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_25.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_23.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_24.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_22.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_23.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_21.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_22.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_20.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_21.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_19.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_20.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_18.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_19.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_17.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_18.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_16.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_17.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_15.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_16.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_14.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_15.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_13.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_14.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_12.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_13.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_11.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_12.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_10.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_11.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_9.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_10.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_8.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_9.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_7.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_8.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_6.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_7.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_5.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_6.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_4.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_5.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_3.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_4.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_2.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_3.jpg")
os.rename("/whirlwind/goes16/vis_color/baja/latest_baja_1.jpg", "/whirlwind/goes16/vis_color/baja/latest_baja_2.jpg")

shutil.copy(filename10, "/whirlwind/goes16/vis_color/baja/latest_baja_1.jpg")


shutil.copy(filename9, "/whirlwind/goes16/vis_color_enh/full/latest_full_1.jpg")

quit()
# Northeast
os.remove("/whirlwind/goes16/vis_color/ne/latest_ne_72.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_71.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_72.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_70.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_71.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_69.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_70.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_68.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_69.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_67.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_68.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_66.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_67.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_65.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_66.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_64.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_65.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_63.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_64.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_62.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_63.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_61.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_62.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_60.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_61.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_59.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_60.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_58.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_59.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_57.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_58.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_56.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_57.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_55.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_56.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_54.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_55.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_53.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_54.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_52.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_53.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_51.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_52.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_50.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_51.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_49.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_50.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_48.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_49.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_47.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_48.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_46.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_47.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_45.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_46.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_44.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_45.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_43.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_44.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_42.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_43.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_41.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_42.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_40.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_41.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_39.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_40.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_38.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_39.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_37.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_38.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_36.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_37.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_35.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_36.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_34.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_35.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_33.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_34.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_32.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_33.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_31.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_32.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_30.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_31.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_29.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_30.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_28.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_29.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_27.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_28.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_26.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_27.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_25.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_26.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_24.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_25.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_23.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_24.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_22.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_23.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_21.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_22.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_20.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_21.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_19.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_20.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_18.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_19.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_17.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_18.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_16.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_17.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_15.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_16.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_14.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_15.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_13.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_14.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_12.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_13.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_11.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_12.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_10.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_11.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_9.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_10.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_8.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_9.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_7.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_8.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_6.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_7.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_5.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_6.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_4.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_5.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_3.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_4.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_2.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_3.jpg")
os.rename("/whirlwind/goes16/vis_color/ne/latest_ne_1.jpg", "/whirlwind/goes16/vis_color/ne/latest_ne_2.jpg")

shutil.copy(filename4, "/whirlwind/goes16/vis_color/ne/latest_ne_1.jpg")

# Madison region
os.remove("/whirlwind/goes16/vis_color/swi/latest_swi_72.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_71.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_72.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_70.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_71.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_69.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_70.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_68.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_69.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_67.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_68.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_66.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_67.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_65.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_66.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_64.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_65.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_63.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_64.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_62.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_63.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_61.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_62.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_60.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_61.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_59.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_60.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_58.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_59.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_57.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_58.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_56.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_57.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_55.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_56.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_54.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_55.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_53.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_54.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_52.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_53.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_51.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_52.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_50.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_51.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_49.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_50.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_48.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_49.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_47.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_48.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_46.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_47.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_45.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_46.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_44.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_45.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_43.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_44.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_42.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_43.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_41.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_42.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_40.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_41.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_39.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_40.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_38.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_39.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_37.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_38.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_36.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_37.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_35.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_36.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_34.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_35.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_33.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_34.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_32.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_33.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_31.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_32.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_30.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_31.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_29.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_30.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_28.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_29.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_27.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_28.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_26.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_27.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_25.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_26.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_24.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_25.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_23.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_24.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_22.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_23.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_21.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_22.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_20.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_21.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_19.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_20.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_18.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_19.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_17.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_18.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_16.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_17.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_15.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_16.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_14.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_15.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_13.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_14.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_12.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_13.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_11.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_12.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_10.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_11.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_9.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_10.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_8.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_9.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_7.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_8.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_6.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_7.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_5.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_6.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_4.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_5.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_3.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_4.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_2.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_3.jpg")
os.rename("/whirlwind/goes16/vis_color/swi/latest_swi_1.jpg", "/whirlwind/goes16/vis_color/swi/latest_swi_2.jpg")

shutil.copy(filename5, "/whirlwind/goes16/vis_color/swi/latest_swi_1.jpg")

# Colorado
os.remove("/whirlwind/goes16/vis_color/co/latest_co_72.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_71.jpg", "/whirlwind/goes16/vis_color/co/latest_co_72.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_70.jpg", "/whirlwind/goes16/vis_color/co/latest_co_71.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_69.jpg", "/whirlwind/goes16/vis_color/co/latest_co_70.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_68.jpg", "/whirlwind/goes16/vis_color/co/latest_co_69.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_67.jpg", "/whirlwind/goes16/vis_color/co/latest_co_68.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_66.jpg", "/whirlwind/goes16/vis_color/co/latest_co_67.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_65.jpg", "/whirlwind/goes16/vis_color/co/latest_co_66.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_64.jpg", "/whirlwind/goes16/vis_color/co/latest_co_65.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_63.jpg", "/whirlwind/goes16/vis_color/co/latest_co_64.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_62.jpg", "/whirlwind/goes16/vis_color/co/latest_co_63.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_61.jpg", "/whirlwind/goes16/vis_color/co/latest_co_62.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_60.jpg", "/whirlwind/goes16/vis_color/co/latest_co_61.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_59.jpg", "/whirlwind/goes16/vis_color/co/latest_co_60.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_58.jpg", "/whirlwind/goes16/vis_color/co/latest_co_59.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_57.jpg", "/whirlwind/goes16/vis_color/co/latest_co_58.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_56.jpg", "/whirlwind/goes16/vis_color/co/latest_co_57.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_55.jpg", "/whirlwind/goes16/vis_color/co/latest_co_56.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_54.jpg", "/whirlwind/goes16/vis_color/co/latest_co_55.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_53.jpg", "/whirlwind/goes16/vis_color/co/latest_co_54.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_52.jpg", "/whirlwind/goes16/vis_color/co/latest_co_53.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_51.jpg", "/whirlwind/goes16/vis_color/co/latest_co_52.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_50.jpg", "/whirlwind/goes16/vis_color/co/latest_co_51.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_49.jpg", "/whirlwind/goes16/vis_color/co/latest_co_50.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_48.jpg", "/whirlwind/goes16/vis_color/co/latest_co_49.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_47.jpg", "/whirlwind/goes16/vis_color/co/latest_co_48.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_46.jpg", "/whirlwind/goes16/vis_color/co/latest_co_47.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_45.jpg", "/whirlwind/goes16/vis_color/co/latest_co_46.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_44.jpg", "/whirlwind/goes16/vis_color/co/latest_co_45.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_43.jpg", "/whirlwind/goes16/vis_color/co/latest_co_44.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_42.jpg", "/whirlwind/goes16/vis_color/co/latest_co_43.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_41.jpg", "/whirlwind/goes16/vis_color/co/latest_co_42.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_40.jpg", "/whirlwind/goes16/vis_color/co/latest_co_41.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_39.jpg", "/whirlwind/goes16/vis_color/co/latest_co_40.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_38.jpg", "/whirlwind/goes16/vis_color/co/latest_co_39.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_37.jpg", "/whirlwind/goes16/vis_color/co/latest_co_38.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_36.jpg", "/whirlwind/goes16/vis_color/co/latest_co_37.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_35.jpg", "/whirlwind/goes16/vis_color/co/latest_co_36.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_34.jpg", "/whirlwind/goes16/vis_color/co/latest_co_35.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_33.jpg", "/whirlwind/goes16/vis_color/co/latest_co_34.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_32.jpg", "/whirlwind/goes16/vis_color/co/latest_co_33.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_31.jpg", "/whirlwind/goes16/vis_color/co/latest_co_32.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_30.jpg", "/whirlwind/goes16/vis_color/co/latest_co_31.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_29.jpg", "/whirlwind/goes16/vis_color/co/latest_co_30.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_28.jpg", "/whirlwind/goes16/vis_color/co/latest_co_29.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_27.jpg", "/whirlwind/goes16/vis_color/co/latest_co_28.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_26.jpg", "/whirlwind/goes16/vis_color/co/latest_co_27.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_25.jpg", "/whirlwind/goes16/vis_color/co/latest_co_26.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_24.jpg", "/whirlwind/goes16/vis_color/co/latest_co_25.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_23.jpg", "/whirlwind/goes16/vis_color/co/latest_co_24.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_22.jpg", "/whirlwind/goes16/vis_color/co/latest_co_23.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_21.jpg", "/whirlwind/goes16/vis_color/co/latest_co_22.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_20.jpg", "/whirlwind/goes16/vis_color/co/latest_co_21.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_19.jpg", "/whirlwind/goes16/vis_color/co/latest_co_20.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_18.jpg", "/whirlwind/goes16/vis_color/co/latest_co_19.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_17.jpg", "/whirlwind/goes16/vis_color/co/latest_co_18.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_16.jpg", "/whirlwind/goes16/vis_color/co/latest_co_17.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_15.jpg", "/whirlwind/goes16/vis_color/co/latest_co_16.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_14.jpg", "/whirlwind/goes16/vis_color/co/latest_co_15.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_13.jpg", "/whirlwind/goes16/vis_color/co/latest_co_14.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_12.jpg", "/whirlwind/goes16/vis_color/co/latest_co_13.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_11.jpg", "/whirlwind/goes16/vis_color/co/latest_co_12.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_10.jpg", "/whirlwind/goes16/vis_color/co/latest_co_11.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_9.jpg", "/whirlwind/goes16/vis_color/co/latest_co_10.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_8.jpg", "/whirlwind/goes16/vis_color/co/latest_co_9.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_7.jpg", "/whirlwind/goes16/vis_color/co/latest_co_8.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_6.jpg", "/whirlwind/goes16/vis_color/co/latest_co_7.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_5.jpg", "/whirlwind/goes16/vis_color/co/latest_co_6.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_4.jpg", "/whirlwind/goes16/vis_color/co/latest_co_5.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_3.jpg", "/whirlwind/goes16/vis_color/co/latest_co_4.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_2.jpg", "/whirlwind/goes16/vis_color/co/latest_co_3.jpg")
os.rename("/whirlwind/goes16/vis_color/co/latest_co_1.jpg", "/whirlwind/goes16/vis_color/co/latest_co_2.jpg")

shutil.copy(filename6, "/whirlwind/goes16/vis_color/co/latest_co_1.jpg")

# Florida
os.remove("/whirlwind/goes16/vis_color/fl/latest_fl_72.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_71.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_72.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_70.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_71.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_69.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_70.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_68.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_69.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_67.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_68.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_66.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_67.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_65.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_66.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_64.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_65.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_63.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_64.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_62.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_63.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_61.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_62.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_60.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_61.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_59.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_60.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_58.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_59.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_57.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_58.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_56.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_57.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_55.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_56.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_54.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_55.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_53.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_54.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_52.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_53.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_51.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_52.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_50.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_51.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_49.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_50.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_48.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_49.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_47.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_48.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_46.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_47.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_45.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_46.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_44.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_45.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_43.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_44.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_42.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_43.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_41.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_42.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_40.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_41.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_39.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_40.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_38.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_39.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_37.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_38.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_36.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_37.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_35.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_36.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_34.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_35.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_33.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_34.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_32.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_33.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_31.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_32.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_30.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_31.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_29.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_30.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_28.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_29.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_27.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_28.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_26.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_27.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_25.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_26.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_24.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_25.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_23.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_24.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_22.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_23.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_21.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_22.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_20.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_21.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_19.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_20.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_18.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_19.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_17.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_18.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_16.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_17.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_15.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_16.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_14.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_15.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_13.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_14.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_12.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_13.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_11.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_12.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_10.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_11.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_9.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_10.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_8.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_9.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_7.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_8.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_6.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_7.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_5.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_6.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_4.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_5.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_3.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_4.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_2.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_3.jpg")
os.rename("/whirlwind/goes16/vis_color/fl/latest_fl_1.jpg", "/whirlwind/goes16/vis_color/fl/latest_fl_2.jpg")

shutil.copy(filename7, "/whirlwind/goes16/vis_color/fl/latest_fl_1.jpg")


# Alex stormchase
os.remove("/whirlwind/goes16/vis_color/alex/latest_alex_72.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_71.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_72.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_70.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_71.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_69.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_70.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_68.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_69.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_67.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_68.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_66.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_67.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_65.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_66.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_64.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_65.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_63.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_64.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_62.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_63.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_61.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_62.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_60.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_61.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_59.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_60.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_58.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_59.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_57.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_58.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_56.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_57.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_55.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_56.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_54.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_55.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_53.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_54.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_52.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_53.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_51.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_52.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_50.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_51.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_49.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_50.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_48.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_49.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_47.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_48.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_46.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_47.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_45.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_46.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_44.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_45.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_43.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_44.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_42.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_43.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_41.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_42.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_40.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_41.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_39.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_40.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_38.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_39.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_37.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_38.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_36.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_37.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_35.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_36.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_34.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_35.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_33.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_34.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_32.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_33.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_31.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_32.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_30.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_31.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_29.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_30.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_28.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_29.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_27.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_28.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_26.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_27.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_25.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_26.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_24.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_25.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_23.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_24.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_22.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_23.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_21.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_22.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_20.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_21.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_19.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_20.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_18.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_19.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_17.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_18.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_16.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_17.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_15.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_16.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_14.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_15.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_13.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_14.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_12.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_13.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_11.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_12.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_10.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_11.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_9.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_10.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_8.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_9.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_7.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_8.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_6.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_7.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_5.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_6.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_4.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_5.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_3.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_4.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_2.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_3.jpg")
os.rename("/whirlwind/goes16/vis_color/alex/latest_alex_1.jpg", "/whirlwind/goes16/vis_color/alex/latest_alex_2.jpg")

shutil.copy(filename10, "/whirlwind/goes16/vis_color/alex/latest_alex_1.jpg")
