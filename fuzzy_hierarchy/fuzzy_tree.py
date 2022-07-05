
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

        print("{}\n{}\n{}\n".format(fa_pos, l_d, l_s))
        idx_s = 0
        node_out = []
        for idx, num_val in enumerate(l_d):
            for ch_num in range(num_val):
                print("fa_pos: {}".format(fa_pos))
                ch_pos = self._add_child(fa_pos[idx], l_s[idx_s])
                idx_s += 1
                print("ch_pos: {}".format(ch_pos))
                print("\n")
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
