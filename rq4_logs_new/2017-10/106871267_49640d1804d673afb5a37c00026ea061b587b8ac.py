# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Evaluation on phantoms
"""

from __future__ import print_function, division, absolute_import, unicode_literals

import nipype.pipeline.engine as pe             # pipeline engine
from nipype.interfaces import utility as niu    # utility
from nipype.algorithms.misc import NormalizeProbabilityMapSet as Normalize

from ..interfaces.utility import ExportSlices
from ..workflows.model import generate_phantom
from .registration import default_regseg
from .evaluation import registration_ev

from builtins import range


def phantoms_wf(options):
    """
    A workflow to generate phantoms and evaluate regseg on
    them
    """

    grid_size = options.grid_size
    if len(grid_size) == 1:
        grid_size = [grid_size] * 3

    bs = bspline(name=options.name, shapes=options.shape,
                 snr_list=options.snr, N=options.repetitions)
    bs.inputs.inputnode.grid_size = grid_size
    bs.inputs.inputnode.lo_matrix = options.lo_matrix
    bs.inputs.inputnode.hi_matrix = options.hi_matrix
    bs.inputs.inputnode.cortex = options.no_cortex
    bs.inputs.inputnode.out_csv = options.out_csv
    return bs


def bspline(name='BSplineEvaluation', shapes=['gyrus'], snr_list=[300], N=1):
    """ A workflow to evaluate registration methods generating a gold standard
    with random bspline deformations.

    A list of nipype workflows can be plugged-in, using the methods input. If
    methods is None, then a default regseg method is run.


      methods = [identity_wf(n_tissues=2), default_regseg()]

    Inputs in methods workflows
    ---------------------------

    methods workflows must define the following inputs:
        inputnode.in_surf - the input prior / surfaces in orig space
        inputnode.in_dist - the distorted images
        inputnode.in_tpms - the distorted TPMs (tissue probability maps)
        inputnode.in_orig - the original images, undistorted


    Outputs in methods workflows
    ----------------------------

        outputnode.out_corr - the distorted images, after correction
        outputnode.out_tpms - the corrected TPMs
        outputnode.out_surf - the original priors after distortion
          (if available)
        outputnode.out_disp - the displacement field, at image grid resoluton

    """
    wf = pe.Workflow(name=name)
    inputnode = pe.Node(niu.IdentityInterface(
        fields=['grid_size', 'out_csv', 'lo_matrix', 'rep_id',
                'hi_matrix', 'snr', 'cortex', 'shape']),
        name='inputnode')

    inputnode.iterables = [('shape', shapes),
                           ('snr', snr_list),
                           ('rep_id', list(range(N))),
    ]

    # inputnode.iterables = [('shape', np.atleast_1d(shapes).tolist()),
    #                        ('snr', snr_list),
    #                        ('rep_id', list(range(N)))]

    outputnode = pe.Node(niu.IdentityInterface(
        fields=['out_file', 'out_tpms', 'out_surfs', 'out_field', 'out_coeff',
                'out_overlap']), name='outputnode')

    phantom = generate_phantom()
    wf.connect([
        (inputnode,  phantom, [('shape', 'inputnode.shape'),
                               ('grid_size', 'inputnode.grid_size'),
                               ('lo_matrix', 'inputnode.lo_matrix'),
                               ('hi_matrix', 'inputnode.hi_matrix'),
                               ('snr', 'inputnode.snr'),
                               ('cortex', 'inputnode.cortex'),
                               ('rep_id', 'inputnode.repetition_id')])
    ])

    regseg_low = default_regseg('REGSEG_low')
    ev_regseg_low = registration_ev(name=('Ev_%s' % regseg_low.name))
    ev_regseg_low.inputs.infonode.method = 'REGSEG'
    ev_regseg_low.inputs.infonode.resolution = 'lo'
    norm_low = pe.Node(Normalize(), name='NormalizeFinal_low')
    export0 = pe.Node(ExportSlices(all_axis=True), name='Export_lo')
    sel0 = pe.Node(niu.Select(index=[0]), name='SelectT1w_lo')

    wf.connect([
        (inputnode, ev_regseg_low, [
            ('shape', 'infonode.shape'),
            ('snr', 'infonode.snr'),
            ('rep_id', 'infonode.repetition')]),
        (phantom, ev_regseg_low, [
            ('refnode.out_signal',    'refnode.in_imag'),
            ('refnode.out_tpms',    'refnode.in_tpms'),
            ('out_lowres.out_surfs',   'refnode.in_surf'),
            ('refnode.out_mask',    'refnode.in_mask'),
            ('out_lowres.out_field', 'refnode.in_field')]),
        (phantom, regseg_low, [
            ('refnode.out_surfs', 'inputnode.in_surf'),
            ('out_lowres.out_signal', 'inputnode.in_fixed'),
            ('out_lowres.out_mask', 'inputnode.in_mask')]),
        # (regseg_low, norm_low, [
        #     ('outputnode.out_tpms', 'in_files')]),
        (regseg_low, ev_regseg_low, [
            ('outputnode.out_corr', 'tstnode.in_imag'),
            ('outputnode.out_surf', 'tstnode.in_surf'),
            ('outputnode.out_field', 'tstnode.in_field')]),
        (norm_low, ev_regseg_low, [
            ('out_files', 'tstnode.in_tpms')]),
        (phantom, sel0, [
            ('out_lowres.out_signal', 'inlist')]),
        (sel0, export0, [
            ('out', 'reference')]),
        (phantom, export0, [
            ('out_lowres.out_surfs', 'sgreen')]),
        (regseg_low, export0, [
            ('outputnode.out_surf', 'syellow')]),
        (inputnode, ev_regseg_low, [('out_csv', 'infonode.out_csv')])
    ])

    regseg_hi = default_regseg('REGSEG_hi')
    ev_regseg_hi = registration_ev(name=('Ev_%s' % regseg_hi.name))
    ev_regseg_hi.inputs.infonode.method = 'REGSEG'
    ev_regseg_hi.inputs.infonode.resolution = 'hi'
    norm_hi = pe.Node(Normalize(), name='NormalizeFinal_hi')
    export1 = pe.Node(ExportSlices(all_axis=True), name='Export_hi')
    sel1 = pe.Node(niu.Select(index=[0]), name='SelectT1w_hi')

    wf.connect([
        (inputnode, ev_regseg_hi, [
            ('shape', 'infonode.shape'),
            ('snr', 'infonode.snr'),
            ('rep_id', 'infonode.repetition')]),
        (phantom, ev_regseg_hi, [
            ('refnode.out_signal',    'refnode.in_imag'),
            ('refnode.out_tpms',    'refnode.in_tpms'),
            ('out_hires.out_surfs',   'refnode.in_surf'),
            ('refnode.out_mask',    'refnode.in_mask'),
            ('out_hires.out_field', 'refnode.in_field')]),
        (phantom, regseg_hi, [
            ('refnode.out_surfs', 'inputnode.in_surf'),
            ('out_hires.out_signal', 'inputnode.in_fixed'),
            ('out_hires.out_mask', 'inputnode.in_mask')]),
        # (regseg_hi, norm_hi, [
        #     ('outputnode.out_tpms', 'in_files')]),
        (regseg_hi, ev_regseg_hi, [
            ('outputnode.out_corr', 'tstnode.in_imag'),
            ('outputnode.out_surf', 'tstnode.in_surf'),
            ('outputnode.out_field', 'tstnode.in_field')]),
        (norm_hi, ev_regseg_hi, [
            ('out_files', 'tstnode.in_tpms')]),
        (phantom, sel1, [
            ('out_hires.out_signal', 'inlist')]),
        (sel0, export1, [
            ('out', 'reference')]),
        (phantom, export1, [
            ('out_hires.out_surfs', 'sgreen')]),
        (regseg_hi, export1, [
            ('outputnode.out_surf', 'syellow')]),
        (inputnode, ev_regseg_hi, [('out_csv', 'infonode.out_csv')])
    ])

    # Connect in_field in case it is an identity workflow
    # if 'in_field' in [item[0] for item in reg.inputs.inputnode.items()]:
    #     wf.connect(phantom, 'out_lowres.out_field',
    #                reg, 'inputnode.in_field')

    return wf