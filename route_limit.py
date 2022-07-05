import os
import re
import yaml
import pandas as pd
import numpy as np
import networkx as nx
from db_con.pcn_oracle_data import PCNDB
from utools.yml_io import read_yaml_to_dict
from tqdm import tqdm


class RoutesLimit():
    def __init__(self, pcnBussInf, channelInfPd, routeDict, pcnDB):
        self.pcnBussInf = pcnBussInf
        self.channelInfPd = channelInfPd
        self.routeDict = routeDict
        self.pcnDB = pcnDB

    ## 获取厂商信息
    def del_business_fac(self, chaList, pcnDB):
        assert isinstance(chaList, list)
        get_value = lambda x: x["NAME"][0] if 'NAME' in x.keys() else None

        facList = [get_value(pcnDB.get_fac_inf(c_id)) for c_id in chaList]
        facList = list(map(self._del_fac_lable, facList))

        conDict = {}
        maxNum, maxName = 0, ''
        for f in facList:
            if f not in conDict.keys():
                conDict[f] = 1
            else:
                conDict[f] += 1
            if conDict[f] > maxNum:
                maxNum = conDict[f]
                maxName = f
        return maxName
        
    def del_channel_fac(self, ch_id, pcnDB):
        get_value = lambda x: x["NAME"][0] if 'NAME' in x.keys() else None
        facList = get_value(pcnDB.get_fac_inf(ch_id))
        facOut = self._del_fac_lable(facList)

        return facOut

    def _del_fac_lable(self, facVal):
        if  not isinstance(facVal, str):
            return 'Others'
        key_words = ['华为', '依赛', '烽火', '马可尼', 'ECI北京', '中兴', '北电', '赛特']
        re_str = '|'.join(key_words)
        
        re_op_matchKeys = lambda x: [re.search(re_str, x), re.search('SDH', x)]
        re_op_delNone = lambda x: x.group(0) if x is not None else ''

        facValList = re_op_matchKeys(facVal)
        res_val = ''.join([re_op_delNone(i) for i in facValList])
        res_val = res_val if len(res_val) != 0 else facVal

        return res_val

    def get_fac_inf_bu(self, pcnBussInf, pcnDB):
        buKey = list(pcnBussInf.keys())
        for b_idx in tqdm(range(len(buKey))):
            b_id = buKey[b_idx]
            busInfDict = pcnBussInf[b_id]
            fac = self.del_business_fac(busInfDict['CHANNELS'], pcnDB)
            busInfDict['FAC_LABLE'] = fac
        
        return pcnBussInf

    def get_fac_inf_ch(self, channelInfPd, pcnDB):
        facSer = pd.Series(data=None, dtype='object')
        for i in tqdm(range(len(channelInfPd))):
            chIloc = channelInfPd.iloc[i]
            ch_id = chIloc['OBJ_ID']
            fac = self.del_channel_fac(ch_id, pcnDB)
            facSer = pd.concat([facSer, pd.Series([fac])], ignore_index=True)
        
        channelInfPd.loc[:,['FAC_LABLE']] = facSer

        return channelInfPd

    def get_fac_inf(self):
        print('\nObtain business-fac Inf.')
        pcnBussInf_fac = self.get_fac_inf_bu(self.pcnBussInf, self.pcnDB)

        print('\nObtain channel-fac Inf.')
        channelInfPd_fac = self.get_fac_inf_ch(self.channelInfPd, self.pcnDB)

        return pcnBussInf_fac, channelInfPd_fac


    ## 获取通道类型和业务类型
    def get_business_type(self, pcnBussInf, pcnDB):
        getInt = lambda x: x[0] if isinstance(x, list) else x
        for b_id, bussInfDict in pcnBussInf.items():
            b_type = pcnDB.get_buss_type(b_id)
            bussInfDict["B_TYPE"] = getInt(b_type['BUZ_TYPE']) if b_type['BUZ_TYPE'] != None else -1

        return pcnBussInf

    def _del_cType(self, cList, channelInfPd):
        cTpyeList = []
        for cha in cList:
            res = channelInfPd.loc[channelInfPd['OBJ_ID'] == cha, "CHANNEL_TYPE"]
            cTpyeList.extend(res.to_list())
        return cTpyeList

    def get_channel_type(self, pcnBussInf, channelInfPd):
        for b_id, bussInfDict in pcnBussInf.items():
            chaList = bussInfDict['CHANNELS']
            cTpyeList = self._del_cType(chaList, channelInfPd)
            cType = cTpyeList[0] if len(cTpyeList) != 0 else '-1'
            bussInfDict['C_TYPE'] = cType

        return pcnBussInf

    def get_buss_cha_inf(self, pcnBussInf):
        print('\nObtain business type Inf.')
        pcnBussInf = self.get_business_type(pcnBussInf, self.pcnDB)
        print('\nObtain channel type Inf.')
        pcnBussInf = self.get_channel_type(pcnBussInf, self.channelInfPd)

        return pcnBussInf

    ## 通道规则制约
    # 扫描站连接属性
    def get_stnLink_attribute(self, channelInfPd, pcnBussInf):
        stnLinkInf = {}
        print('Obtin Station Inf.')
        print("*  Collect stn-channel type information")
        for i in tqdm(range(len(channelInfPd))):
            chInfIloc = channelInfPd.iloc[i]
            aStn, zStn = chInfIloc['A_STATION_NAME'], chInfIloc['Z_STATION_NAME']
            cType = chInfIloc['CHANNEL_TYPE']
            facType = chInfIloc['FAC_LABLE']
            stnKey = '{}-{}'.format(aStn, zStn)
            if stnKey not in stnLinkInf.keys():
                stnLinkInf[stnKey] = {'CTYPE': set([cType]), 'FTYPE': set([facType])}
            else:
                stnLinkInf[stnKey]['CTYPE'] = stnLinkInf[stnKey]['CTYPE'] | set([cType])
                stnLinkInf[stnKey]['FTYPE'] = stnLinkInf[stnKey]['FTYPE'] | set([facType])

        print("*  Collect stn-business type information")
        buKey = list(pcnBussInf.keys())
        for b_idx in tqdm(range(len(buKey))):
            b_id = buKey[b_idx]
            bussInf = pcnBussInf[b_id]
            aStn, zStn = bussInf['A_STATION_NAME'], bussInf['Z_STATION_NAME']
            bType = bussInf['B_TYPE']
            # cType = chInfIloc['CHANNEL_TYPE']
            stnKey = '{}-{}'.format(aStn, zStn)
            if stnKey not in stnLinkInf.keys():
                stnLinkInf[stnKey] = {'BTYPE': set(bType)}
            else:
                if 'BTYPE' in stnLinkInf[stnKey].keys():
                    stnLinkInf[stnKey]['BTYPE'] = stnLinkInf[stnKey]['BTYPE'] | set([bType])  
                else:
                    stnLinkInf[stnKey]['BTYPE'] = set([bType])
            
        return stnLinkInf

    def filter_routes(self, routeDict, pcnBussInf, stnLinkInf):
        print('Filter Routes with channel rules.')
        b_idList = list(routeDict.keys())
        TypeRule = read_yaml_to_dict('./conf/limit_rule.yml')
        routeLimitDict = {}
        for b_idx in tqdm(range(len(b_idList))):
            b_id = b_idList[b_idx]
            routes = routeDict[b_id]
            TypeDict = dict([(key, pcnBussInf[b_id][key]) for key in ['FAC_LABLE', 'B_TYPE', 'C_TYPE']])
            routesLimit = self._del_routes_limit(routes, TypeDict, TypeRule, stnLinkInf)
            routeLimitDict[b_id] = routesLimit
        return routeLimitDict

    def _del_routes_limit(self, routes, TypeDict, TypeRule, stnLinkInf):
        routesOut = []
        for routeList in routes:
            RoutePass = False
            if len(routeList) == 2:
                continue
            for a_idx in range(len(routeList) - 1):
                z_idx = a_idx + 1
                aStn, zStn = routeList[a_idx], routeList[z_idx]
                stnKey = '{}-{}'.format(aStn, zStn)
                stnKey1 = '{}-{}'.format(zStn, aStn)
                stnInfAZ = stnLinkInf[stnKey] if stnKey in stnLinkInf.keys() else stnLinkInf[stnKey1]
                stnInfAZ1 = stnLinkInf[stnKey1] if stnKey1 in stnLinkInf.keys() else stnLinkInf[stnKey]
                stnInfAZ.update(stnInfAZ1)

                #BTYPE
                if 'BTYPE' in stnInfAZ.keys():
                    bTypeRule = TypeRule['BTYPE']
                    stnBType = stnInfAZ['BTYPE']
                    bType = TypeDict['B_TYPE']
                    bRes = stnBType & set(bTypeRule[bType])
                    if len(bRes) == 0:
                        break

                #CTYPE
                cTypeRule = TypeRule['CTYPE']
                stnCType = stnInfAZ['CTYPE']
                cType = TypeDict['C_TYPE']
                cRes = stnCType & set(cTypeRule[cType])
                if len(cRes) == 0:
                    break

                #FAC_LABLE
                if 'FTYPE' in stnInfAZ.keys():
                    facType = TypeDict['FAC_LABLE']
                    if facType == 'Others' or 'Others' in stnInfAZ['FTYPE']:
                        continue
                    if facType not in stnInfAZ['FTYPE']:
                        break
                if zStn == routeList[-1]:
                    RoutePass = True

            if RoutePass == True:
                routesOut.append(routeList)
        return routesOut
    
    def get_route_limit(self):
        pcnBussInf, channelInfPd = self.get_fac_inf()
        pcnBussInf = self.get_buss_cha_inf(pcnBussInf)

        stnLinkInf = self.get_stnLink_attribute(channelInfPd, pcnBussInf)
        routeLimitDict = self.filter_routes(self.routeDict, pcnBussInf, stnLinkInf)

        bussLen, sumLen = 0, 0
        for _, routes in self.routeDict.items():
            if len(routes) != 0:
                sumLen += len(routes)
                bussLen += 1
        meanLen = sumLen/bussLen if bussLen != 0 else 0

        bussLenLimit, sumLenLimit = 0, 0
        for _, routes in routeLimitDict.items():
            if len(routes) != 0:
                sumLenLimit += len(routes)
                bussLenLimit += 1
        meanLenLimit = sumLenLimit/bussLenLimit if bussLen != 0 else 0

        print(' Filter results '.center(50, '*'))
        print('* Before Filter:')
        print('**  Len of bussiniess:{}'.format(bussLen))
        print('**  Mean of routes:{:.2f}'.format(meanLen))
        print('* After Filter:')
        print('**   Len of bussiniess:{}'.format(bussLenLimit))
        print('**   Mean of routes:{:.2f}'.format(meanLenLimit))
        print(' Filtering complete '.center(50, '*'))

        return routeLimitDict
            


if __name__ == '__main__':
    from utools.pickle_io import load_obj, save_obj
    from db_con.pcn_oracle_data import PCNDB
    pcnBussInf = load_obj('pcnBussInf')
    channelInfPd = load_obj('channelInfPd')
    routeDict = load_obj('routeDict')

    pcnDBInf = read_yaml_to_dict('./conf/pcnDB_inf.yml')
    pcnDB = PCNDB(pcnDBInf['USER'], pcnDBInf['PASS'], pcnDBInf['SID'])
    testRL = RoutesLimit(pcnBussInf, channelInfPd, routeDict, pcnDB)

    routeLimitDict = testRL.get_route_limit()
    save_obj(routeLimitDict, 'routeLimitDict')


