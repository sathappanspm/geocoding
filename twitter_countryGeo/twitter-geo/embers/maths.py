#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - @todo
"""

__author__ = 'Patrick Butler'
__email__ = 'pbutler@killertux.org'

import numpy as np
from scipy import integrate


def patch_holes(z, dt=60):
    def patch_hole(v, i, dt):
        t0, t1 = v[i:i + 2, 0]
        td = t1 - t0
        if td / 2. > dt:
            t = t0 + np.arange(int((t1 - t0) / dt) - 1) * dt + dt
            z = np.zeros((t.shape[0], 2))
            z[:, 0] += t
            z = np.vstack((v[:i + 1], z,  v[i + 1:]))
        else:
            z = np.vstack((v[:i + 1], [[(t0 + t1) / 2., 0]],  v[i + 1:]))
        return z

    zd = np.diff(z[:, 0])
    i = np.argmax(zd)
    z2 = z
    while zd[i] > dt * 2:
        z2 = patch_hole(z2, i, dt)
        zd = np.diff(z2[:, 0])
        i = np.argmax(zd)
    return z2


def normalize_to_interval(z, newdt=60):
    z = patch_holes(z, newdt)
    cum = np.r_[[0], integrate.cumtrapz(z[:, 1], z[:, 0])]
    t0, t1 = z[0, 0], z[-1, 0]
    td = t1 - t0
    x = np.linspace(t0 + 60 - (t0 % 60), t1 - (t1 % 60),  1 + td // 60)
    cum_int = np.interp(x, z[:, 0], cum, 0, 0)
    return np.c_[x[1:], np.diff(cum_int) / 60]
