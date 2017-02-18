#!/usr/bin/env python
# encoding: utf-8
"""
Utils.py

Various methods that are used by other methods in the ANC module.

Created by Geoff Cureton on 2013-03-04.
Copyright (c) 2013 University of Wisconsin SSEC. All rights reserved.
Licensed under GNU GPLv3.
"""

import logging
import string
import numpy as np
from bisect import bisect_left, bisect_right

# every module should have a LOG object
LOG = logging.getLogger('Utils')


def index(a, x):
    '''Locate the leftmost value exactly equal to x'''
    i = bisect_left(a, x)
    if i != len(a) and a[i] == x:
        return i
    raise ValueError


def find_lt(a, x):
    '''Find rightmost value less than x'''
    i = bisect_left(a, x)
    if i:
        return a[i - 1]
    raise ValueError


def find_le(a, x):
    '''Find rightmost value less than or equal to x'''
    i = bisect_right(a, x)
    if i:
        return a[i - 1]
    raise ValueError


def find_gt(a, x):
    '''Find leftmost value greater than x'''
    i = bisect_right(a, x)
    if i != len(a):
        return a[i]
    raise ValueError


def find_ge(a, x):
    '''Find leftmost item greater than or equal to x'''
    i = bisect_left(a, x)
    if i != len(a):
        return a[i]
    raise ValueError


def findDatelineCrossings(latCrnList, lonCrnList):
    '''
    Finds the places where the boundary points that will make up a polygon
    cross the dateline.

    This method is heavily based on the AltNN NNfind_crossings() method

    NOTE:  This loop will find the place(s) where the boundary crosses 180
    degrees longitude.  It will also record the index after the crossing
    for the first two crossings.

    NOTE:  Since the last point in the boundary is equal to the first point
    in the boundary, there is no chance of a crossing between the last
    and first points.

    initialize the number of crossings to zero
    for loop over the boundary
       if the longitudes cross the 180 degree line, then
          increment the number of crossings
          if this is first crossing, then
             save the index after the crossing
          else if this is the second crossing
             save the index after the second crossing
          endif
       endif
    end for loop
    '''

    status = 0
    numCrosses = 0
    cross1Idx_ = None
    cross2Idx_ = None

    # For an ascending granule, the corner points are numbered [0,1,3,2], from the southeast
    # corner moving anti-clockwise.

    LOG.debug("latCrnList = {}".format(latCrnList))
    LOG.debug("lonCrnList = {}".format(lonCrnList))

    for idx1, idx2 in zip([1, 3, 2], [0, 1, 3]):

        # Convert the longitudes to radians, and calculate the
        # absolute difference
        lon1 = np.radians(lonCrnList[idx1])
        lon2 = np.radians(lonCrnList[idx2])
        lonDiff = np.fabs(lon1 - lon2)

        if (np.fabs(lonDiff) > np.pi):

            # We have a crossing, incrememnt the number of crossings
            numCrosses += 1

            if(numCrosses == 1):

                # This was the first crossing
                cross1Idx_ = idx1

            elif(numCrosses == 2):

                # This was the second crossing
                cross2Idx_ = idx1

            else:

                # we should never get here
                status = -1
                return status

    LOG.debug("cross1Idx_ = {}".format(cross1Idx_))
    LOG.debug("cross2Idx_ = {}".format(cross2Idx_))

    num180Crossings_ = numCrosses

    '''
    # now determine the minimum and maximum latitude
    maxLat_ = latCrnList[0]
    minLat_ = maxLat_

    for idx in [1,3,2]:
        if(latCrnList[idx] > maxLat_):
            # if current lat is bigger than maxLat_, make the current point the
            # maximum
            maxLat_ = latCrnList[idx]

        if(latCrnList[idx] < minLat_):
            # if current lat is smaller than minLat_, make the current point the
            # minimum
            minLat_ = latCrnList[idx]

    return num180Crossings_,minLat_,maxLat_
    '''

    return num180Crossings_


def plotArr(data, pngName, vmin=None, vmax=None):
    '''
    Plot the input array, with a colourbar.
    '''

    # Plotting stuff
    import matplotlib
    import matplotlib.cm as cm
    from matplotlib.colors import ListedColormap
    from matplotlib.figure import Figure

    matplotlib.use('Agg')
    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

    # This must come *after* the backend is specified.
    import matplotlib.pyplot as ppl

    LOG.info("Plotting a GridIP dataset {}".format(pngName))

    plotTitle = string.replace(pngName, ".png", "")
    cbTitle = "Value"
    #vmin,vmax =  0,1

    # Create figure with default size, and create canvas to draw on
    scale = 1.5
    fig = Figure(figsize=(scale * 8, scale * 3))
    canvas = FigureCanvas(fig)

    # Create main axes instance, leaving room for colorbar at bottom,
    # and also get the Bbox of the axes instance
    ax_rect = [0.05, 0.18, 0.9, 0.75]  # [left,bottom,width,height]
    ax = fig.add_axes(ax_rect)

    # Granule axis title
    ax_title = ppl.setp(ax, title=plotTitle)
    ppl.setp(ax_title, fontsize=12)
    ppl.setp(ax_title, family="sans-serif")

    # Plot the data
    im = ax.imshow(data, axes=ax, interpolation='nearest', vmin=vmin, vmax=vmax)

    # add a colorbar axis
    cax_rect = [0.05, 0.05, 0.9, 0.10]  # [left,bottom,width,height]
    cax = fig.add_axes(cax_rect, frameon=False)  # setup colorbar axes

    # Plot the colorbar.
    cb = fig.colorbar(im, cax=cax, orientation='horizontal')
    ppl.setp(cax.get_xticklabels(), fontsize=9)

    # Colourbar title
    cax_title = ppl.setp(cax, title=cbTitle)
    ppl.setp(cax_title, fontsize=9)

    # Redraw the figure
    canvas.draw()

    # save image
    canvas.print_figure(pngName, dpi=200)
