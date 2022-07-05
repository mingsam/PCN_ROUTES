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
