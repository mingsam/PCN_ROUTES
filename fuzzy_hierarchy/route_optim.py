import numpy as np
import pandas as pd
import re
import os
from tqdm import tqdm

# file_path = 'C:\\Users\\78442\\Desktop\\数据文件'
# file_path = '../../data'
# os.chdir(file_path)
# os.getcwd()


class RouteOptim:
    def __init__(self, pd_data=None, pd_lroute=None):
        # self.pd_f = pd.read_csv("T_FIBER.csv")
        # self.pd_fs = pd.read_csv("T_FIBER_SEG.csv")
        # self.pd_fl = pd.read_csv("T_FIBER_LINE.csv")
        self.pd_f = None
        self.pd_fs = None
        self.pd_fl = None
        self.pd_tss = pd.read_csv("T_SPC_SITE.csv" , low_memory=False)
        self.station_az =  np.load('station_dir.npy', allow_pickle=True).item()
        if pd_data is None:
            self.pd_data = pd.read_csv("LINK_AZ_v6_1129.csv")
        else:
            self.pd_data = pd_data
        if pd_lroute is None:
            self.pd_lroute = pd.read_csv("LIMITED_ROUTE.csv")
        else:
            self.pd_lroute = pd_lroute

    def get_link_inf(self):
        global se_link
        pd_f = self.pd_data
        pd_route_t = self.pd_lroute

        res_out = []
        link_s_out = []
        # for i in range(len(pd_route_t)):
        for i in tqdm(range(len(pd_route_t))):

            iloc_p_rt = pd_route_t.iloc[i]
            limit_r = eval(iloc_p_rt["LIMIT_ROUTE"])
            fac_label = pd_f.iloc[i]["FAC_LABEL"]

            # station_inf = np.array([self.get_station_type1(iloc_p_rt["A_STATION"]),
            #                               self.get_station_type1(iloc_p_rt["Z_STATION"])])
            route_s = []
            link_s = []
            # print("strat:{} route".format(i))
            # print(limit_r)
            for route in limit_r:
                # 单个备用通道 计算路由分数
                #             print(route)
                route_inf = np.zeros(3)
                se_link = []

                for n in range(len(route) - 1):
                    # 获取A-B站所有路由
                    # route_l = []
                    # route_type = []
                    # route_l, route_type = self.f_a_z_link(route[n], route[n + 1])
                    route_l, route_type = self.f_a_z_link_v2(route[n], route[n + 1], fac_label)

                    se_id, route_inf_s = self.get_link_s(route_l, route_type, route[n], route[n + 1])

                    route_inf = np.add(route_inf, route_inf_s)
                    # print(route_inf_s)
                    # print(route_inf)
                    if len(route_l) != 0:
                        se_link.append(route_l[se_id])
                    else:
                        se_link.append([])
                # print(route_inf)
                if len(route) != 0:
                    route_inf[0:2] /= route_inf[-1]
                # print(route_inf)
                route_s.append(route_inf)
                link_s.extend(se_link)
            route_s = np.array(route_s)
            res_out.append(route_s)
            link_s_out.append(link_s)

        return res_out, link_s_out

    def get_link_s(self, route_l, route_type, a_s, z_s):
        route_inf_s = []
        # print(route_l, route_type)
        s_station = np.array([self.get_station_type1(a_s), self.get_station_type1(z_s)])
        s_dsp_type = max([self.get_station_dsp_level(a_s), self.get_station_dsp_level(z_s)])
        link_inf = [s_dsp_type, s_station.max(), 1]
        route_inf_s.append(link_inf)
        # for (link_list, c_type) in zip(route_l, route_type):
        #     #         print(link_list, c_type)
        #     link_inf = np.zeros((1, 3))
        #     if c_type == 2:
        #         if len(link_list) == 0:
        #             s_station = np.array([self.get_station_type1(a_s), self.get_station_type1(z_s)])
        #             s_dsp_type = max([self.get_station_dsp_level(a_s), self.get_station_dsp_level(z_s)])
        #             link_inf = [s_dsp_type, s_station.max(), 1]
        #         else:
        #             s_station = np.array([self.get_station_type1(a_s), self.get_station_type1(z_s)])
        #             s_dsp_type = max([self.get_station_dsp_level(a_s), self.get_station_dsp_level(z_s)])
        #             # p_type, fiber_l = [], []
        #             # for i in range(len(link_list) // 2):
        #             #     p_type.append(link_list[2 * i])
        #             #     if link_list[2 * i] == 4:
        #             #         fiber_l.append(link_list[2 * i + 1])
        #             #                 print(fiber_l)
        #             # fiber_inf = np.array(list(map(self.get_fiber_inf, fiber_l)))
        #
        #             # print(fiber_inf)
        #             # link_inf = np.array([s_dsp_type, fiber_inf[:, 1].max(), 1])
        #             link_inf = [s_dsp_type, s_station.max(), 1]
        #
        #     else:
        #         s_station = np.array([self.get_station_type1(a_s), self.get_station_type1(z_s)])
        #         s_dsp_type = max([self.get_station_dsp_level(a_s), self.get_station_dsp_level(z_s)])
        #         link_inf = [s_dsp_type, s_station.max(), 1]
        #
        #     route_inf_s.append(link_inf)

        se_idx = 0
        # for i in range(len(route_inf_s)):
        #     if route_inf_s[i][1] <= route_inf_s[se_idx][1]:
        #         se_idx = i

        # print(se_idx, len(route_inf_s), route_inf_s[0])
        # print(se_idx, len(route_inf_s))
        return se_idx, np.array(route_inf_s[se_idx])

    def get_fiber_inf(self, line_obj_id):
        pd_fl = self.pd_fl
        pd_fs = self.pd_fs
        pd_f = self.pd_f
        res_fl = pd_fl.loc[pd_fl["OBJ_ID"] == line_obj_id, ["OBJ_ID", "PAR_FIBER_SEG"]]
        res_t1 = pd.merge(res_fl, pd_fs, how='inner', left_on=["PAR_FIBER_SEG"], right_on=["OBJ_ID"])
        res_t2 = pd.merge(res_t1, pd_f, how='left', left_on=["PAR_FIBER", "LENGTH"], right_on=["OBJ_ID", "LENGTH"])

        f_lenght = 5.5
        f_level = 5
        f_dsp = 3
        if len(res_t2) > 0:
            f_lenght = res_t2["LENGTH"].values[0]
            f_level = res_t2["VOLTAGE_CLASS"].values[0]
            f_dsp = res_t2["DSP_LEVEL_x"].values[0]

        return [f_dsp, f_level]

    def get_station_type(self, s):
        pd_tss = self.pd_tss
        res_t = pd_tss.loc[pd_tss["OBJ_ID"] == s]["VOLTAGE_CLASS"]
        if len(res_t) != 0:
            res_out = res_t.values[0]
            res_out = 15 - res_out
            if res_out == 0:
                return 15
            else:
                return res_out
        else:
            return 0

    def get_station_type1(self, s):
        pd_tss = self.pd_tss
        res_t = pd_tss.loc[pd_tss["OBJ_ID"] == s]["VOLTAGE_CLASS"]
        if len(res_t) != 0:
            res_out = res_t.values[0]
            res_out = res_out + 1
            if res_out == 16:
                return 1
            else:
                return res_out
        else:
            return 15

    def get_station_dsp_level(self, s):
        pd_tss = self.pd_tss
        res_t = pd_tss.loc[pd_tss["OBJ_ID"] == s]["DSP_LEVEL"]
        if len(res_t) != 0:
            res_out = res_t.values[0]
            if np.isnan(res_out):
                return 3
            else:
                return res_out
        else:
            return 6

    def f_a_z_link(self, a_s, z_s):
        pd_tmp_t = self.pd_data
        res_t = pd_tmp_t.loc[((pd_tmp_t["A_STATION"] == a_s) & (pd_tmp_t["Z_STATION"] == z_s)) | (
                    (pd_tmp_t["A_STATION"] == z_s) & (pd_tmp_t["Z_STATION"] == a_s))]
        res_t_link = res_t["LINK"]
        res_t_c_type = res_t["C_TYPE"]

        res_link_out = list(map(eval, res_t_link.values))

        #     for val in res_t_link.values:
        #         res_link_out.append(eval(val))

        return res_link_out, res_t_c_type.values

    def f_a_z_link_v2(self, a_s, z_s, fac_label):
        pd_tmp_t = self.pd_data
        pd_lroute = self.pd_lroute
        station_az = self.station_az
        if fac_label in ['Empty0', 'Empty1', 'Empty2']:
            label = 'Other'
        else:
            label = fac_label
        az_name = a_s + ' ' + z_s + label
        az_name2 = z_s + ' ' + a_s + label
        if az_name in station_az.keys():
            az_name_s = az_name
            res_link = pd_tmp_t.loc[station_az[az_name_s], 'LINK']
            res_t_c_type = pd_tmp_t.loc[station_az[az_name_s], 'C_TYPE']
        elif az_name2 in station_az.keys():
            az_name_s = az_name2
            res_link = pd_tmp_t.loc[station_az[az_name_s], 'LINK']
            res_t_c_type = pd_tmp_t.loc[station_az[az_name_s], 'C_TYPE']
        else:
            res_t = pd_tmp_t.loc[((pd_tmp_t["A_STATION"] == a_s) & (pd_tmp_t["Z_STATION"] == z_s)) | (
                    (pd_tmp_t["A_STATION"] == z_s) & (pd_tmp_t["Z_STATION"] == a_s))]
            res_link = res_t["LINK"]
            res_t_c_type = res_t["C_TYPE"]

        res_link_out = list(map(eval, res_link.values))
        return res_link_out, res_t_c_type.values


if __name__ == "__main__":
    file_path = '../../data'
    os.chdir(file_path)
    os.getcwd()
    rt = RouteOptim()
    r_s, r = rt.get_link_inf()
    pd_tmp = pd.DataFrame(columns=["score", "link"])
    # print(len(r_s), len(r))
    for i in range(len(r_s)):
        val_s = r_s[i]
        val_s = val_s.tolist()
        val = r[i]
        pd_t = pd.DataFrame(data={"score": str(val_s), "link": str(val)}, index=[0])
        pd_tmp = pd_tmp.append(pd_t, ignore_index=True)

    pd_tmp.to_csv("ROUTE_SCORE.csv")

