from math import inf
from copy import copy
from collections import Counter
from functools import reduce
import numpy as np
from .base import Heuristic, memo


class NewCAS(Heuristic):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pld = np.full(self.N, False)
        self.edge_tasks = None

        self.RP = np.array(self._RT)
        self.TD = np.full((self.N, self.N), -1, dtype=int)
        self.TC = [{t} for t in self.problem.tasks]
        for t in reversed(self._topsort()):
            self.calculate_RP(t)

    def rt_sum(self, ts):
        return sum(self._RT[t.id] for t in ts)

    def td(self, ti, tj):
        if self.TD[ti.id, tj.id] < 0:
            mts = [t for t in tj.prevs() if self.is_successor(ti, t) or ti == t]
            self.TD[ti.id, tj.id] = max(
                self.td(ti, t) + self.td(t, tj) for t in mts)
        return self.TD[ti.id, tj.id]

    @memo
    def ra(self, task):
        return max([self.ra(t) + self.TD[t.id, task.id] for t in task.prevs()],
                   default=self._RT[task.id])

    def _eft(self, task):
        if not task.in_degree:
            return -inf, -inf
        elif not task.out_degree:
            return inf, inf
        else:
            return -self.RP[task.id], -sum(self.CT(c) for c in task.in_comms)

    def sort_tasks(self):
        self.scheduling_list = sorted(self.problem.tasks, key=self._eft)
        for task in self.scheduling_list:
            self.edge_tasks = set(
                t for t in self.problem.tasks
                if self._pld[t.id] and
                any(not self._pld[_t.id] for _t in t.succs() if _t != task))
            yield task
            self._pld[task.id] = True

    def default_fitness(self):
        return inf, inf, [inf]

    def fitness(self, task, machine, comm_pls, st):
        ft_w = st + self.RP[task.id]
        all_ft = []
        cur_ft = st + self.RP[task.id]
        for t in self.edge_tasks:
            if self.PL_m(t) == machine:
                fti, ftj = self.RP2s(t, task, self.ST(t), st)
            else:
                rpi, rpj = self.RP2m(t, task)
                fti, ftj = self.ST(t) + rpi, st + rpj
            ft_w = max(ft_w, fti, ftj)
            all_ft.append(fti)
            cur_ft = max(cur_ft, ftj)
        all_ft.append(cur_ft)
        return ft_w, st, sorted(all_ft, reverse=True)

    def calculate_RP(self, task):
        self.TD[task.id, task.id] = 0
        st_t = st_c = self._RT[task.id]
        for t in sorted(task.succs(), key=lambda _t: -self.RP[_t.id]):
            ct = self._CT[task.id, t.id]
            t_l = max(self.rt_sum(self.TC[task.id] | self.TC[t.id]),
                      st_t + self.RP[t.id])
            t_r = st_c + ct + self.RP[t.id]
            if (t_l, st_t) <= (t_r, st_c + ct):
                st_t += self._RT[t.id]
                self.TC[task.id] |= self.TC[t.id]
                self.TD[task.id, t.id] = st_t - self._RT[task.id]
                self.RP[task.id] = max(self.RP[task.id], t_l)
            else:
                st_c += ct
                self.TD[task.id, t.id] = st_c + \
                    self._RT[t.id] - self._RT[task.id]
                self.RP[task.id] = max(self.RP[task.id], t_r)

    def RP2s(self, ti, tj, sti, stj):
        rpi, rpj = self.RP[ti.id], self.RP[tj.id]
        if sti < stj:
            rpi = max(rpi, self.rt_sum(self.TC[ti.id] | self.TC[tj.id]))
        else:
            rpj = max(rpj, self.rt_sum(self.TC[ti.id] | self.TC[tj.id]))
        return sti + rpi, stj + rpj

    def rp2m_est2(self, ti, task, ft, tm, ct):
        flag = False
        for t in sorted(filter(tm.__contains__, task.prevs()),
                        key=lambda _t: self.td(ti, _t)):
            flag = True
            ct = max(ct, ft + self.td(ti, t)) + self._CT[t.id, task.id]
        return ct, flag

    @memo
    def RP2m(self, ti, tj):
        tmi, tmj = copy(self.TC[ti.id]), copy(self.TC[tj.id])
        ci, cj = fti, ftj = self.ra(ti), self.ra(tj)
        r = 0
        for t in self.scheduling_list:
            if t == tj:
                tmi.discard(t)
            elif t == ti or self._RT[t.id] == 0:
                tmj.discard(t)
            elif t in tmi and t in tmj:
                tci, fi = self.rp2m_est2(ti, t, fti, tmi, ci)
                tcj, fj = self.rp2m_est2(tj, t, ftj, tmj, cj)
                if not fi:
                    tmi.discard(t)
                elif not fj:
                    tmj.discard(t)
                elif tcj <= tci:
                    r = max(r, tcj + self.RP[t.id])
                    cj = max(cj, tcj)
                    tmj.discard(t)
                else:
                    r = max(r, tci + self.RP[t.id])
                    ci = max(ci, tci)
                    tmi.discard(t)
        rpmi = max(self.RP[ti.id], r - fti + self._RT[ti.id])
        rpmj = max(self.RP[tj.id], r - ftj + self._RT[tj.id])
        return rpmi, rpmj


class NewCAS_RA(NewCAS):
    @memo
    def RP2m(self, ti, tj):
        tmi, tmj = self.TC[ti.id] - {tj}, self.TC[tj.id] - {ti}
        ci, cj = fti, ftj = self.ra(ti), self.ra(tj)
        r = 0
        for t in self.scheduling_list:
            if self._RT[t.id] and t in tmi and t in tmj:
                pti = sorted(filter(tmi.__contains__, t.prevs()), key=self.ra)
                ptj = sorted(filter(tmj.__contains__, t.prevs()), key=self.ra)
                if not pti:
                    tmi.discard(t)
                elif not ptj:
                    tmj.discard(t)
                else:
                    def add_comm(ct, _t):
                        return max(ct, self.ra(_t)) + self._CT[_t.id, t.id]
                    tci = reduce(add_comm, pti, ci)
                    tcj = reduce(add_comm, ptj, cj)
                    if tcj <= tci:
                        r = max(r, tcj + self.RP[t.id])
                        cj = tcj
                        tmj.discard(t)
                    else:
                        r = max(r, tci + self.RP[t.id])
                        ci = tci
                        tmi.discard(t)
        rpi = max(self.RP[ti.id], r - fti + self._RT[ti.id])
        rpj = max(self.RP[tj.id], r - ftj + self._RT[tj.id])
        return rpi, rpj


class NewCAS_RA2(NewCAS_RA):
    @memo
    def ra(self, task):
        st = 0
        A = np.empty(self.N, int)
        B = np.empty(self.N, int)
        for t in sorted(task.prevs(), key=self.ra):
            A[t.id] = max(self.ra(t) - st, 0) + self._CT[t.id, task.id]
            B[t.id] = max(st - self.ra(t), 0)
            st += A[t.id]
        k, d = st, 0
        for t in sorted(task.prevs(), key=self.ra, reverse=True):
            d = max(d, min(A[t.id], k))
            k = min(k, B[t.id])
        return st + self._RT[task.id] - d


class NewCAS_RP2(NewCAS_RA):
    def calculate_RP(self, task):
        st_t = st_c = self._RT[task.id]
        self.TD[task.id, task.id] = 0
        for t in sorted(task.succs(), key=lambda _t: -self.RP[_t.id]):
            t_l = max(self.RP2s(task, t, 0, st_t))
            st_r = st_c + self._CT[task.id, t.id]
            t_r = max(self.RP[task.id], st_r + self.RP[t.id])
            if (t_l, st_t) <= (t_r, st_r):
                st_t += self._RT[t.id]
                self.RP[task.id] = t_l
                self.TD[task.id, t.id] = st_t - self._RT[task.id]
                self.TC[task.id] |= self.TC[t.id]
            else:
                st_c = st_r
                self.RP[task.id] = t_r
                self.TD[task.id, t.id] = st_c + \
                    self._RT[t.id] - self._RT[task.id]


class NewCAS_m2(NewCAS):
    def rp2m_est2(self, ti, task, ft, tm, ct, lt):
        flag = False
        for t in sorted(filter(tm.__contains__, task.prevs()),
                        key=lambda _t: self.td(ti, _t)):
            flag = True
            ct = max(ct, ft + self.td(ti, t)) + self._CT[t.id, task.id]
            lt = max(lt, ft + self.td(ti, t))
        return lt, ct, flag

    @memo
    def RP2m(self, ti, tj):
        tmi, tmj = copy(self.TC[ti.id]), copy(self.TC[tj.id])
        li, lj = ci, cj = fti, ftj = self.ra(ti), self.ra(tj)
        r = 0
        for t in self.scheduling_list:
            if t == tj:
                tmi.discard(t)
            elif t == ti or self._RT[t.id] == 0:
                tmj.discard(t)
            elif t in tmi and t in tmj:
                tli, tci, fi = self.rp2m_est2(ti, t, fti, tmi, ci, li)
                tlj, tcj, fj = self.rp2m_est2(tj, t, ftj, tmj, cj, lj)
                if not fi:
                    tmi.discard(t)
                elif not fj:
                    tmj.discard(t)
                elif max(tli, tcj) <= max(tlj, tci):
                    r = max(r, max(tli, tcj) + self.RP[t.id])
                    li, cj = tli, tcj
                    tmj.discard(t)
                else:
                    r = max(r, max(tlj, tci) + self.RP[t.id])
                    lj, ci = tlj, tci
                    tmi.discard(t)
        rpmi = max(self.RP[ti.id], r - fti + self._RT[ti.id])
        rpmj = max(self.RP[tj.id], r - ftj + self._RT[tj.id])
        return rpmi, rpmj


class NewCAS_m2RA(NewCAS):
    @memo
    def RP2m(self, ti, tj):
        tmi, tmj = self.TC[ti.id] - {tj}, self.TC[tj.id] - {ti}
        li, lj = ci, cj = fti, ftj = self.ra(ti), self.ra(tj)
        r = 0
        for t in self.scheduling_list:
            if self._RT[t.id] and t in tmi and t in tmj:
                pti = sorted(filter(tmi.__contains__, t.prevs()), key=self.ra)
                ptj = sorted(filter(tmj.__contains__, t.prevs()), key=self.ra)
                if not pti:
                    tmi.discard(t)
                elif not ptj:
                    tmj.discard(t)
                else:
                    def add_comm(ct, _t):
                        return max(ct, self.ra(_t)) + self._CT[_t.id, t.id]
                    tli = max(map(self.ra, pti))
                    tci = reduce(add_comm, pti, ci)
                    tlj = max(map(self.ra, ptj))
                    tcj = reduce(add_comm, ptj, cj)
                    if max(tli, tcj) <= max(tlj, tci):
                        r = max(r, max(tli, tcj) + self.RP[t.id])
                        li, cj = tli, tcj
                        tmj.discard(t)
                    else:
                        r = max(r, max(tlj, tci) + self.RP[t.id])
                        lj, ci = tlj, tci
                        tmi.discard(t)
        rpi = max(self.RP[ti.id], r - fti + self._RT[ti.id])
        rpj = max(self.RP[tj.id], r - ftj + self._RT[tj.id])
        return rpi, rpj


class NewCAS_m3RA(NewCAS):
    @memo
    def RP2m(self, ti, tj):
        tmi, tmj = self.TC[ti.id] - {tj}, self.TC[tj.id] - {ti}
        li, lj = ci, cj = fti, ftj = self.ra(ti), self.ra(tj)
        r = 0
        for t in self.scheduling_list:
            if self._RT[t.id] and t in tmi and t in tmj:
                pti = sorted(filter(tmi.__contains__, t.prevs()), key=self.ra)
                ptj = sorted(filter(tmj.__contains__, t.prevs()), key=self.ra)
                if not pti:
                    tmi.discard(t)
                elif not ptj:
                    tmj.discard(t)
                else:
                    def add_comm(ct, _t):
                        return max(ct, self.ra(_t)) + self._CT[_t.id, t.id]
                    tci = reduce(add_comm, pti, ci)
                    tcj = reduce(add_comm, ptj, cj)
                    tli = max(self.ra(_t) for _t in t.prevs() if t not in ptj)
                    tlj = max(self.ra(_t) for _t in t.prevs() if t not in pti)
                    if max(tli, tcj) <= max(tlj, tci):
                        r = max(r, max(tli, tcj) + self.RP[t.id])
                        li, cj = tli, tcj
                        tmj.discard(t)
                    else:
                        r = max(r, max(tlj, tci) + self.RP[t.id])
                        lj, ci = tlj, tci
                        tmi.discard(t)
        rpi = max(self.RP[ti.id], r - fti + self._RT[ti.id])
        rpj = max(self.RP[tj.id], r - ftj + self._RT[tj.id])
        return rpi, rpj