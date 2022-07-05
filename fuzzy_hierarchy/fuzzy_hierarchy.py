from typing import Optional, Any

import numpy as np
import pandas as pd
import os
import sys
import re

# from fuzzy_tree import *
# from route_score import *

# file_path = 'C:\\Users\\78442\\Desktop\\数据文件'
# file_path = '../../data'
# os.chdir(file_path)
# os.getcwd()


import numpy as np


class BaseTree:
    def __init__(self):
        self._root_node = None
        self.treeSize = 0

    class Node:
        def __init__(self, val, parent=None):
            self.val = val
            self.parent = parent
            self.children = []

    class Position:
        def __init__(self, container, node):
            self.container = container
            self.node = node

        def element(self):
            return self.node.element

        def __eq__(self, other):
            return isinstance(other, type(self)) and other.node is self.node

    def _validate(self, p):
        if not isinstance(p, self.Position):
            raise TypeError('p must be proper Position type')
        if p.node.parent is p.node:
            raise ValueError('p is no longer valid')
        return p.node

    def _make_position(self, node):
        return self.Position(self, node) if node is not None else None

    def _root(self):
        return self._make_position(self._root_node)

    def _parent(self, p):
        node = self._validate(p)
        return self._make_position(node.parent)

    def _children(self, p):
        node = self._validate(p)
        return [self._make_position(child) for child in node.children]

    def _child(self, p, child_num):
        node = self._validate(p)
        if child_num > len(node.children):
            raise ValueError('child_num is longer than node.children.size')
        return self._make_position(node.children[child_num])

    def _add_child(self, p, e):
        #         if e is None:
        #             return None
        node = self._validate(p)
        #         print("\nfa_node: {}".format(node))
        #         print("fa_node.ch: {}".format(node.children))
        self.treeSize += 1
        ch_n = self.Node(val=e, parent=node)
        #         print("ch_node: {}".format(ch_n))
        #         print("ch_node.ch: {}".format(ch_n.children))
        node.children.append(ch_n)
        #         print("fa_node.ch_a: {}".format(node.children))
        #         print("ch_node.ch_a: {}\n".format(ch_n.children))
        return self._make_position(node.children[-1])

    def add_root(self, e):
        if self._root_node is not None:
            raise ValueError('Root exists')
        self.treeSize += 1
        self._root_node = self.Node(e)
        return self._make_position(self._root_node)


class WeightTree(BaseTree):
    def __init__(self):
        super(WeightTree, self).__init__()
        self._root_node = self.add_root(0)
        self.leaf_node = []


class LayerRelTree(BaseTree):
    """
    data_in: layer_num: int
             layer_digit: list(Evaluation matrix dimension; Breadth first search)
             layer_sorce: list(Evaluation matrix; Breadth first search)

    data_out: Tree data structure

    """

    def __init__(self, layer_num, layer_digit, layer_s):
        super(LayerRelTree, self).__init__()
        self.layer_n = layer_num
        self.layer_d = layer_digit
        self.layer_s = layer_s

    def _gene_layertree(self, fa_pos, l_d, l_s):
        if not isinstance(fa_pos, list):
            fa_pos = [fa_pos]

        # print("{}\n{}\n{}\n".format(fa_pos, l_d, l_s))
        idx_s = 0
        node_out = []
        for idx, num_val in enumerate(l_d):
            for ch_num in range(num_val):
                # print("fa_pos: {}".format(fa_pos))
                ch_pos = self._add_child(fa_pos[idx], l_s[idx_s])
                idx_s += 1
                # print("ch_pos: {}".format(ch_pos))
                # print("\n")
                node_out.append(ch_pos)
        return node_out

    def calc_layertree(self):
        self._root_node = self.add_root(0)
        opt_p = self._root_node
        idx_ld_old, idx_ld = 0, 0
        idx_ls = 0
        for l_num in range(self.layer_n):
            ch_num = self.layer_d[idx_ld_old]
            idx_ld += ch_num
            l_d = self.layer_d[idx_ld:idx_ld + ch_num]
            node_num = np.sum(l_d)
            l_s = self.layer_s[idx_ls:idx_ls + node_num]
            opt_p = self._gene_layertree(opt_p, l_d, l_s)
            idx_ld_old = idx_ld
            idx_ls += node_num


class RouteScore:
    def __init__(self, route_limit, aph_s):
        # self.ch_val = ch_val
        self.route_limit = route_limit

        # self.route_optim = route_optim
        self.aph_s = aph_s

    def getData(self):
        layer_num = 1

        # ch_val_s = self.ch_val.shape[0]
        # route_limit_s = self.route_limit.shape[0]
        # route_optim_s = self.route_optim.shape[0]

        # layer_digit = [1, 1, ch_val_s, route_limit_s, route_optim_s]
        # layer_digit = [1, 1, 3, 0, 2, 3]
        # score = [self.ch_val, self.route_limit, self.route_optim]

        layer_digit = [1, 1, 3]
        score = self.route_limit

        return layer_num, layer_digit, self.aph_s, score


class FuzzyAHP:
    def __init__(self, ahp_sorce, layer_rel, layer_w, layer_s):
        self.ahp_s = ahp_sorce
        self.layer_r = layer_rel
        self.layer_w = WeightTree()
        self.layer_s = layer_s

    # + - * / of fuzzy triangle
    # in : m_a/m_b(numpy array)  n*3
    def _fuzzy_mul(self, m_a, m_b):
        m_a = m_a.reshape(-1, 3)
        m_b = m_b.reshape(-1, 3)

        m_tmp = np.divide(m_a, m_b)
        m_tmp2 = np.divide(m_a, m_b[:, ::-1])
        m_calu = np.concatenate((m_tmp[:, (0, 2)], m_tmp2[:, (0, 2)]), axis=1)
        m_tmp[:, 0] = np.min(m_calu, aixs=1).reshape(-1, 1)
        m_tmp[:, 1] = np.max(m_calu, aixs=1).reshape(-1, 1)

        return m_tmp

    def _fuzzy_div(self, m_a, m_b):
        m_a = m_a.reshape(-1, 3) + 0.000001
        m_b = m_b.reshape(-1, 3) + 0.000001

        m_tmp = np.multiply(m_a, m_b)
        m_tmp2 = np.multipy(m_a, m_b[:, ::-1])

    def _fuzzy_add(self, m_a, m_b):
        m_a = m_a.reshape(-1, 3)
        m_b = m_b.reshape(-1, 3)

        m_tmp = np.add(m_a, m_b)
        return m_tmp

    def _fuzzy_sub(self, m_a, m_b):
        m_a = m_a.reshape(-1, 3)
        m_b = m_b.reshape(-1, 3)

        m_tmp = np.subtract(m_a, m_b)
        return m_tmp

    # Deblurring operation
    # in:a_bar(numpy array; [l,m,r])
    def _defuzzy(self, a_bar):
        l, m, r = a_bar[0], a_bar[1], a_bar[2]
        xa_abr = (l + m + 2 * r) / 4.0
        sigmaa_bar = (l - r) ** 2 + (l - m) ** 2 + 2 * (m - r) ** 2
        sigmaa_bar = (sigmaa_bar / 80) ** 0.5

        l_min = np.ones(l.shape) * np.min(l)
        r_max = np.ones(r.shape) * np.max(r)
        Ea_bar = 1 - (2 * (r - l)) / (3 * (r_max - l_min))
        rhoa_bar = Ea_bar * xa_abr + (Ea_bar - 1) * sigmaa_bar

        return rhoa_bar

    # LLSM (Logarithmic Least Square Method)
    # in:layer_r(numpy array) 3*n*n
    def _llsm(self, layer_r):

        #         print("layer_r: {}".format(layer_r))
        #         print("layer_r.shape: {}".format(layer_r.shape))

        if len(layer_r.shape) < 3:
            #             print("ttt")
            layer_r = self._fuzzy_ex(layer_r)
        #         print("socre.shape:{}".format(layer_r.shape))
        (l_fuz, l_row, l_col) = layer_r.shape
        w_list = []
        w_lm, w_rm, w_mm = 0, 0, 0

        #         print("llsm_in: {}".format(layer_r))
        for idx_n in range(l_row):
            w_l = np.prod(layer_r[2]) ** (1 / l_row / l_row) * np.prod(layer_r[0][idx_n]) ** (1 / l_row)
            w_lm += np.prod(layer_r[2][idx_n]) ** (1 / l_row)
            #             print("test_out:{}".format(np.prod(layer_r[2]))
            #             print((np.prod(layer_r[2]) ** (1 / l_row / l_row), np.prod(layer_r[0][idx_n]) ** (1 / l_row)))

            w_m = np.prod(layer_r[1][idx_n]) ** (1 / l_row)
            w_mm += w_m

            w_r = np.prod(layer_r[0]) ** (1 / l_row / l_row) * np.prod(layer_r[2][idx_n]) ** (1 / l_row)
            w_rm += np.prod(layer_r[0][idx_n]) ** (1 / l_row)
            #             print((np.prod(layer_r[0]) ** (1 / l_row / l_row), np.prod(layer_r[2][idx_n]) ** (1 / l_row)))

            #             print("w_l:{}\tw_m:{}\tw_r:{}\n".format(w_l, w_m, w_r))

            w_list.append([w_l, w_m, w_r])
        w_list = np.array(w_list)
        #         print("w_lm:{}\nw_rm:{}\n".format(w_lm, w_rm))
        w_list[:, 0] /= w_lm
        w_list[:, 1] /= w_mm
        w_list[:, 2] /= w_rm

        #         print("w_list:{}".format(w_list))
        return w_list

    def _fuzzy_ex(self, s):
        d = 0.25
        (l_len, r_len) = s.shape
        l_s = np.ones((l_len, r_len))
        r_s = np.ones((l_len, r_len))
        for l in range(l_len):
            for r in range(r_len):
                tmp = s[l][r]
                if tmp == 1:
                    continue
                elif tmp > 1:
                    l_s[l][r] = s[l][r] - d * tmp
                    r_s[l][r] = s[l][r] + d * tmp
                elif tmp < 1:
                    l_s[l][r] = 1 / (1 / s[l][r] + d * tmp)
                    r_s[l][r] = 1 / (1 / s[l][r] - d * tmp)

                #                 tmp  = 0.5
        #                 if l < r:
        #                     l_s[l][r] = s[l][r] - d * tmp
        #                     r_s[l][r] = s[l][r] + d * tmp
        #                 elif l > r:
        #                     l_s[l][r] = 1 / (s[l][r] + d * tmp)
        #                     r_s[l][r] = 1 / (s[l][r] - d * tmp)

        res = np.array([l_s, s, r_s])
        return res

    def getFuzzy_weight(self):
        lr_tree = self.layer_r
        op_p = lr_tree._root_node
        num_node = 0
        while num_node < lr_tree.treeSzie:
            lr_ch = lr_tree._children(op_p)
            num_node += len(lr_ch)
            # for ch in lr_ch:
            return None

    def getFuzzyWeight(self):
        lr_tree = self.layer_r
        lw_tree = self.layer_w

        opt_lr = lr_tree._root_node
        opt_lw = lw_tree._root_node
        lr_ch_l = [opt_lr]
        lw_ch_l = [opt_lw]

        while lw_tree.treeSize < lr_tree.treeSize:
            tmp_lr_l, tmp_lw_l = [], []
            idx_lw = 0
            for opt in lr_ch_l:
                opt_w = lw_ch_l[idx_lw]
                #                 print(opt_w)
                tmp_lr, tmp_lw = self._calc_LlsmWeight(lr_tree, lw_tree, opt, opt_w)
                tmp_lr_l += tmp_lr
                tmp_lw_l += tmp_lw
                idx_lw += 1
            lr_ch_l = tmp_lr_l
            lw_ch_l = tmp_lw_l

        self.layer_w.leaf_node = lw_ch_l

        return lw_tree

    def _calc_LlsmWeight(self, lr_tree, lw_tree, opt_lr, opt_lw):
        lw_ch_l = []
        lr_ch_l = lr_tree._children(opt_lr)
        for lr_ch in lr_ch_l:

            #             print("lr_ch.node.val:{}".format(lr_ch.node.val))
            if lr_ch.node.val is None:
                val = None
            else:
                val = self._llsm(lr_ch.node.val)
            lw_ch = lw_tree._add_child(opt_lw, val)
            lw_ch_l.append(lw_ch)

        return lr_ch_l, lw_ch_l

    def calc_relia(self):
        #         print(self.layer_s)
        l_sorce = self.layer_s
        s_l = []
        idx = 0
        for leaf_node in self.layer_w.leaf_node[0].node.children:
            #             tmp_s = self._fuzzy_mul(l_sorce[idx], leaf_node.node.val)
            val = leaf_node.val
            if val is None:
                val = np.ones(l_sorce[idx].shape)
            #             print(val)
            #             print("l_sorce[idx]:{}".format(l_sorce[idx]))
            route_s = []
            for i in range(l_sorce.shape[0]):
                one_r_s = []
                for n in range(l_sorce.shape[1]):
                    one_r_s.append(val[n] * l_sorce[i][n])
                route_s.append(one_r_s)
            route_s = np.array(route_s)

            [num_route, num_factor, num_dim] = route_s.shape

            de_res = []
            for n_r in range(num_route):
                r_l = []
                for n_f in range(num_factor):
                    dz_f = self._defuzzy(route_s[n_r][n_f])
                    r_l.append(dz_f)
                de_res.append(r_l)
            de_res = np.array(de_res)

            res_out_l = np.sum(de_res, axis=1)
            res_idx = np.argmin(res_out_l)

            #             tmp_s = np.sum(tmp_s, axis=0)
            #             s_l.append(tmp_s)
            #             idx += 1

            #         weight = self.layer_w._child(self.layer_w._root_node, 0).node.val
            #         res = np.sum(np.array(s_l) * weight, axis=0)

            #         res_defuzzy = self._defuzzy(res)
            #         return route_s, de_res, res_out_l, res_idx
            return res_out_l, res_idx


def get_score(route_s):
    res_out = []
    res_idx_out = []
    for val in route_s:
        if len(val) == 0:
            res = np.array([])
            idx = np.array([])
            res_out.append(res)
            res_idx_out.append(idx)
            continue

        ahp_1r = np.array(([[1, 5, 2], [1/5, 1, 1/4], [1/2, 4, 1]]))
        ahp_21r = None
        ahp_22r = np.array([[1., 0.167, 4.], [6., 1., 7.], [0.25, 0.143, 1.]])
        ahp_23r = np.array([[1., 0.33], [3., 1.]])
        ahp_s = [ahp_1r, ahp_21r, ahp_22r, ahp_23r]

        route_limit = np.array(val)

        t_data = RouteScore( route_limit,  ahp_s)
        layer_num, layer_digit, aph_s, sorce = t_data.getData()

        # print(layer_num, layer_digit, aph_s)
        ts = LayerRelTree(layer_num, layer_digit, aph_s)
        ts.calc_layertree()
        layer_w = WeightTree()
        fz = FuzzyAHP(aph_s, ts, layer_w, sorce)
        fz.getFuzzyWeight()
        fz.getFuzzyWeight()

        res, idx = fz.calc_relia()
        res_out.append(res)
        res_idx_out.append(idx)
    #         print(res)
    return res_out, res_idx_out
# pd_tmp = pd.read_csv("ROUTE_SCORE.csv")
# route_s = list(map(eval, pd_tmp["score"]))
#
# res_out, res_idx_out = get_score(route_s)
# print(res_out, res_idx_out)
#

# if __name__ == "main":
    # # main TEST
    # ahp_1r = np.array(([[1, 3, 5], [0.33, 1, 4], [0.2, 0.25, 1]]))
    # ahp_21r = None
    # ahp_22r = np.array([[1., 0.167, 4.], [6., 1., 7.], [0.25, 0.143, 1.]])
    # ahp_23r = np.array([[1., 0.33], [3., 1.]])
    # ahp_s = [ahp_1r, ahp_21r, ahp_22r, ahp_23r]
    # #
    # route_limit = np.array([[0.5, 1.5, 1.75], [0.5, 1, 1.75], [0.5, 1.5, 1.5], [0.5, 2.5, 3.75]])
    # #
    # # t_data = RouteScore( route_limit, ahp_1r)
    # # layer_num, layer_digit, aph_s, score = t_data.getData()
    # #
    # # ts = LayerRelTree(layer_num, layer_digit, aph_s)
    # # res = ts.calc_layertree()
    # # fz = FuzzyAHP(aph_s, ts, score)
    # # lw = fz.getFuzzyWeight()
    # # lw_rr = fz.getFuzzyWeight()
    # #
    # # res_t = fz.calc_relia()
    # test = FuzzyAHP(ahp_s)
    # tt, tt1 = test.get_score(route_limit)
    # print(tt, tt1)




