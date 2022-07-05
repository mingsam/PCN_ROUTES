import os
import yaml
import pandas as pd
import numpy as np
import networkx as nx
from db_con.pcn_oracle_data import PCNDB
from utools.yml_io import read_yaml_to_dict
from tqdm import tqdm


class RouteOpti():
    def __init__(self, routeLimitDict, stnDict):
        stnInfPd = pd.DataFrame(data=stnDict)
        stnInfPd = stnInfPd.T.reset_index(drop=True)
        self.stnInfPd = stnInfPd
        self.routeLimitDict = routeLimitDict
    
    def get_route_attr(self, route, stnInfPd, attr):
        attrRes = []
        for stn in route:
            resAttr = stnInfPd.loc[stnInfPd['STN_NAME'] == stn, attr].values
            if len(resAttr) == 0:
                resAttr_int = 5 if attr == 'VOLTAGE_CLASS' else 3
            elif resAttr[0] is None:
                resAttr_int = 4
            else:
                resAttr_int = float(resAttr[0])
                if attr == 'VOLTAGE_CLASS':
                    resAttr_int = 0.5 if resAttr_int == 15 else resAttr_int
                    resAttr_int = 5 if resAttr_int == 16 else resAttr_int
            attrRes.append(resAttr_int)

        return np.array(attrRes, dtype=np.float64)

    # 获取路由跳数
    def get_routes_steps(self, routeLimitDict):
        print('\nObtain routes Steps Inf.')
        routeStepsDict = {}
        b_keyList = list(routeLimitDict.keys())
        for b_idx in tqdm(range(len(b_keyList))):
            b_id = b_keyList[b_idx]
            routes = routeLimitDict[b_id]
            routeSteps = []
            for route in routes:
                rSteps = len(route)
                routeSteps.append(rSteps)
            routeStepsDict[b_id] = np.array(routeSteps)

        return routeStepsDict

    # 获取路由调度等级
    def get_routes_DSP(self, routeLimitDict, stnInfPd):
        print('\nObtain routes DSP Inf.')
        routeDSPDict = {}
        b_keyList = list(routeLimitDict.keys())
        for b_idx in tqdm(range(len(b_keyList))):
            b_id = b_keyList[b_idx]
            routes = routeLimitDict[b_id]
            routesDSP = []
            for route in routes:
                rDSPs = self.get_route_attr(route, stnInfPd, 'VOLTAGE_CLASS')
                routesDSP.append(rDSPs)
            routeDSPDict[b_id] = routesDSP

        return routeDSPDict 

    # 获取站点电压等级
    def get_route_Vol(self, routeLimitDict, stnInfPd):
        print('\nObtain routes Vol Inf.')
        routeVolDict = {}
        b_keyList = list(routeLimitDict.keys())
        for b_idx in tqdm(range(len(b_keyList))):
            b_id = b_keyList[b_idx]
            routes = routeLimitDict[b_id]
            routesVol = []
            for route in routes:
                rVol = self.get_route_attr(route, stnInfPd, 'DSP_LEVEL')
                routesVol.append(rVol)
                
            routeVolDict[b_id] = routesVol

        return routeVolDict

    def route_optim(self, routeStepsDict, routeDSPDict, routeVolDict):
        ## 直接调用模糊层次分析模块
        print('Call fuzzy_hierarchy Module.')
        routeScoreDict = {}
        b_key = routeStepsDict.keys()
        print("*  Start to optimize routes.")
        for b_id in b_key:
            DSPList, VolList = routeDSPDict[b_id], routeVolDict[b_id]
            DSPm = np.array([i.mean() for i in DSPList])
            Volm = np.array([i.mean() for i in VolList])
            Steps = routeStepsDict[b_id]
            
            Weight = read_yaml_to_dict('./conf/factor_weight.yml')
            scores = DSPm * Weight['DSP_LEVEL'] + Volm * Weight['VOLTAGE_CLASS'] + Steps * Weight['STEP']

            routeScoreDict[b_id] = scores
            

        return routeScoreDict

    # 输出最佳路由
    def get_optimum_route(self, routeScoreDict, routeLimitDict):
        print('*  RES_OUT of Fuzzy hierarchy.')
        print('Result output')
        optRoutes = {}
        for b_id in routeScoreDict:
            routeLimit = routeLimitDict[b_id]
            routeScore = routeScoreDict[b_id]

            if len(routeLimit) > 0:
                minIdx = np.argmin(routeScore)
                optRoute = routeLimit[minIdx]
            else:
                optRoute = None

            optRoutes[b_id] = optRoute

        return optRoutes

    def get_optimun_routes(self):
        routeLimitDict, stnInfPd = self.routeLimitDict, self.stnInfPd
        routeStepsDict = self.get_routes_steps(routeLimitDict)
        routeDSPDict = self.get_routes_DSP(routeLimitDict, stnInfPd)
        routeVolDict = self.get_route_Vol(routeLimitDict, stnInfPd)

        routeScoreDict = self.route_optim(routeStepsDict, routeDSPDict, routeVolDict)
        optRoutes = self.get_optimum_route(routeScoreDict, routeLimitDict)

        return optRoutes, routeScoreDict


if __name__ == '__main__':
    from utools.pickle_io import load_obj, save_obj
    from db_con.pcn_oracle_data import PCNDB
    routeLimitDict = load_obj('routeLimitDict')
    stnDict = load_obj('stnDict')

    testRO =  RouteOpti(routeLimitDict, stnDict)
    optRoutes, routeScoreDict = testRO.get_optimun_routes()


    print(len(optRoutes.keys()))

    save_obj(optRoutes, 'optRoutes')
    save_obj(routeScoreDict, 'routeScoreDict')


