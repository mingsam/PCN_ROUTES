import imp
import os
import pandas as pd
import numpy as np
import networkx as nx
from db_con.pcn_oracle_data import PCNDB
from utools.yml_io import read_yaml_to_dict
from tqdm import tqdm


class PCNROUTE():
    def __init__(self, pcnDBInf):
        self.pcnDBInf = pcnDBInf
        self.pcnDB = self._init_pcndb()

        
    def _init_pcndb(self):
        pcnDBInf = self.pcnDBInf
        assert 'USER' in pcnDBInf.keys()
        assert 'PASS' in pcnDBInf.keys()
        assert 'SID' in pcnDBInf.keys()
        pcnDB = PCNDB(pcnDBInf['USER'], pcnDBInf['PASS'], pcnDBInf['SID'])

        print(" PCN DataBase INF ".center(50, '*'))
        for k in pcnDBInf.keys():
            if k != 'PASS':
                print("\t* {}: {}".format(k, pcnDBInf[k]))
        print(" Successfully connected! ".center(50, '*')+'\n')        

        return pcnDB


    def _trans_stn_name(self, stn_id, pcnDB, stnDict):
        pcnDB = self.pcnDB
        del_full_name = lambda x:x[0].split('/')[-1] if x[0] != None else None
        if stn_id not in stnDict.keys():
            stn_dict = pcnDB.get_station_inf(stn_id, tCols=['FULL_NAME', 'VOLTAGE_CLASS', 'DSP_LEVEL'])
            stn_name = del_full_name(stn_dict['FULL_NAME']) if stn_dict['FULL_NAME'] != None else None
            vol_class = del_full_name(stn_dict['VOLTAGE_CLASS']) if stn_dict['VOLTAGE_CLASS'] != None else None
            dsp_level = del_full_name(stn_dict['DSP_LEVEL']) if stn_dict['DSP_LEVEL'] != None else None
            stnDict[stn_id] = {
                'STN_ID': stn_id,
                'STN_NAME': stn_name,
                'VOLTAGE_CLASS': vol_class,
                'DSP_LEVEL': dsp_level
            }
        else:
            stn_name = stnDict[stn_id]['STN_NAME']
        
        return stn_name, stnDict


    def ch_channel_name(self, channelInf):
        pcnDB = self.pcnDB
        assert 'A_STATION' in channelInf.keys() and 'Z_STATION' in channelInf.keys()
        stnList_a = channelInf['A_STATION']
        stnList_z = channelInf['Z_STATION']

        stnList_a_name , stnList_z_name = [], []
        resDict = {}

        for idx, (s_a, s_z) in enumerate(zip(stnList_a, stnList_z)):
            s_a_name, resDict = self._trans_stn_name(s_a, pcnDB, resDict)
            s_z_name, resDict = self._trans_stn_name(s_z, pcnDB, resDict)

            stnList_a_name.append(s_a_name)
            stnList_z_name.append(s_z_name)

        channelInf['A_STATION_NAME'] = stnList_a_name
        channelInf['Z_STATION_NAME'] = stnList_z_name

        return channelInf, resDict


    def del_channel_inf(self, channelInf):
        pcnDB = self.pcnDB
        print("Start processing channel information!")
        print("\t*Replace site name")
        channelInf, stnDict = self.ch_channel_name(channelInf)
        stnList_a = channelInf['A_STATION_NAME']
        stnList_z = channelInf['Z_STATION_NAME']

        assert len(stnList_a) == len(stnList_z)

        print("\t*Raw data: {}".format(len(stnList_a)))
        idx = 0
        while idx < len(stnList_a):
            s_a = stnList_a[idx]
            s_z = stnList_z[idx]
            if s_a is None or s_z is None:
                for k in channelInf.keys():
                    channelInf[k].pop(idx)
            elif s_a == s_z:
                for k in channelInf.keys():
                    channelInf[k].pop(idx)
            else:
                idx += 1

        print("\t*channelInf __len__(): {}".format(idx))
        print("Complete channel information processing!\n")

        channelInfPd = pd.DataFrame(data=channelInf)
        return channelInfPd, stnDict


    def con_business_inf(self, b_id, pcnDB, stnDict):
        busInfDict = pcnDB.get_business_inf(b_id)
        stnID_a, stnID_z = busInfDict['A_SITE_ID'][0], busInfDict['Z_SITE_ID'][0]

        stnName_a = self._trans_stn_name(stnID_a, pcnDB, stnDict)
        stnName_z = self._trans_stn_name(stnID_z, pcnDB, stnDict)
        busInfDict['A_STATION_NAME'] = stnName_a
        busInfDict['Z_STATION_NAME'] = stnName_z

        return busInfDict


    def con_business_stn_inf(self, b_id, stnDict):
        pcnDB = self.pcnDB
        resDitc={}
        busInfDict = pcnDB.get_business_inf(b_id)
        stnID_a, stnID_z = busInfDict['A_SITE_ID'][0], busInfDict['Z_SITE_ID'][0]

        stnName_a,_ = self._trans_stn_name(stnID_a, pcnDB, stnDict)
        stnName_z,_ = self._trans_stn_name(stnID_z, pcnDB, stnDict)
        resDitc['A_STATION_NAME'] = stnName_a
        resDitc['Z_STATION_NAME'] = stnName_z

        return resDitc


    def del_business_inf(self, businessInf, stnDict=None):
        pcnDb = self.pcnDB
        assert 'BUSINESS_ID' in businessInf.keys() and 'CHANNEL_ID' in businessInf

        print("Start processing business information!")
        busDict = {}
        for idx, busID in enumerate(businessInf['BUSINESS_ID']):
            if busID not in busDict.keys():
                busDict[busID] = [businessInf['CHANNEL_ID'][idx]]
            else:
                busDict[busID].append(businessInf['CHANNEL_ID'][idx])

        businessInfDict ={}
        for idx, b_id in enumerate(list(busDict.keys())):
            busInfDict = self.con_business_stn_inf(b_id, stnDict)
            businessInfDict[b_id] = busInfDict

        print("\t*available business :{}".format(len(businessInfDict.keys())))
        print("Complete processing business information!\n")

        return busDict, businessInfDict


    def generate_pcnMap(self, channelInf):
        pcnMap = nx.Graph()
        stnList_a = channelInf['A_STATION_NAME']
        stnList_z = channelInf['Z_STATION_NAME']
        channelIdList = channelInf['OBJ_ID']

        edgeDict = {}
        print("Start to contract pcnMap.")
        for idx, (c_id, s_a, s_z) in enumerate(zip(channelIdList, stnList_a, stnList_z)):
            edgeStr = '{}-{}'.format(s_a, s_z)
            edgeStrRe = '{}-{}'.format(s_z, s_a)
            if edgeStr in edgeDict.keys():
                edgeDict[edgeStr].append(c_id)
            elif edgeStrRe in edgeDict.keys():
                edgeDict[edgeStrRe].append(c_id)
            else:
                pcnMap.add_edge(s_a, s_z)
                edgeDict[edgeStr] = [c_id]

        for e in pcnMap.edges.keys():
            edgekey = '{}-{}'.format(e[0], e[1])
            edgekeyRe = '{}-{}'.format(e[1], e[0])
            if edgekey in edgeDict.keys():
                pcnMap.edges[e]['channel_id'] = edgeDict[edgekey]
            else:
                pcnMap.edges[e]['channel_id'] = edgeDict[edgekeyRe]

        mapDegree = np.array([d for _, d in pcnMap.degree])
        print("Finish  contract pcnMap.")
        print(" MAP INF ".center(50, "*"))
        print('\t* Nodes num:{}'.format(len(pcnMap.nodes)))
        print('\t* Edges num:{}'.format(len(pcnMap.edges)))
        print('\t* Degree max:{}'.format(mapDegree.max()))
        print('\t* Degree min:{}'.format(mapDegree.min()))
        print('\t* Degree mean:{:.2f}'.format(mapDegree.mean()))
        print(" MAP INF ".center(50,'*')+'\n')
        return pcnMap


    def get_circuitous_bus(self, businessInfDict, pcnMap):
        busDelDict={}
        for idx,b_id in enumerate(businessInfDict.keys()):
            stnName_a = businessInfDict[b_id]['A_STATION_NAME']
            stnName_z = businessInfDict[b_id]['Z_STATION_NAME']

            if stnName_a not in pcnMap.nodes.keys() or stnName_z not in pcnMap.nodes.keys():
                continue
            elif stnName_a is None or stnName_z is None:
                continue
            elif stnName_a == stnName_z:
                continue
            else:
                busDelDict[b_id] = businessInfDict[b_id]

        return busDelDict

    
    def get_access_routes(self, busDelDict, pcnMap, cutoff=3):
        resDict={}
        routeDict = {}
        busIDList = list(busDelDict.keys())
        print("Start to query for available routes!")
        for idx in tqdm(range(len(busDelDict))):
            b_id = busIDList[idx]
            stn_a, stn_z = busDelDict[b_id]['A_STATION_NAME'], busDelDict[b_id]['Z_STATION_NAME']
            resKey = "{}-{}".format(stn_a, stn_z)
            resKey1 = "{}-{}".format(stn_z, stn_a)
            if resKey in resDict.keys():
                routeDict[b_id] = resDict[resKey]
            elif resKey1 in resDict.keys():
                routeDict[b_id] = resDict[resKey1]
            else:
                nx_allRoutes = nx.all_simple_paths(pcnMap,source=stn_a,target=stn_z, cutoff=cutoff)
                routes = list(nx_allRoutes)
                routeDict[b_id] = routes
                resDict[resKey] = routes

        return routeDict

    def get_circuitous_inf(self, busInfDict, businessInfDict, pcnMap):
        pcnBussInf={}
        for _,b_id in enumerate(businessInfDict.keys()):
            stnName_a = businessInfDict[b_id]['A_STATION_NAME']
            stnName_z = businessInfDict[b_id]['Z_STATION_NAME']

            if stnName_a not in pcnMap.nodes.keys() or stnName_z not in pcnMap.nodes.keys():
                continue
            elif stnName_a is None or stnName_z is None:
                continue
            elif stnName_a == stnName_z:
                continue
            else:
                pcnBussInf[b_id] = businessInfDict[b_id]
        
        for b_id in pcnBussInf.keys():
            if b_id in busInfDict.keys():
                pcnBussInf[b_id]['CHANNELS'] = busInfDict[b_id]
        
        return pcnBussInf
    
    def get_all_routes(self, DataStorage=False):
        pcnDB = self.pcnDB

        print("\nStart data acquisition. ")
        bussinesses_inf = pcnDB.get_all_businesses()
        channels_inf = pcnDB.get_all_channels()
        print("Complete data acquisition.\n")


        channelInfPd, stnDict = self.del_channel_inf(channels_inf)
        busInfDict, businessInfDict = self.del_business_inf(bussinesses_inf, stnDict)

        pcnMap = self.generate_pcnMap(channelInfPd)
        busDelDict = self.get_circuitous_bus(businessInfDict, pcnMap)
        routeDict = self.get_access_routes(busDelDict, pcnMap)
        pcnBussInf =  self.get_circuitous_inf(busInfDict, businessInfDict, pcnMap)


        if DataStorage:
            # self.busInfDict = busInfDict
            # self.businessInfDict = businessInfDict
            self.channelInfPd = channelInfPd
            self.pcnBussInf = pcnBussInf
            self.pcnMap = pcnMap
            self.routeDict = routeDict
            self.stnDict = stnDict

            return pcnBussInf, channelInfPd, stnDict, routeDict
        else:
            return routeDict



if __name__ =='__main__':
    # pcnDBInf={
    #     'USER': 'PCN_TEST',
    #     'PASS': '784427618',
    #     'SID': 'localhost:1521/LHR11G'
    # }
    from utools.pickle_io import *
    pcnDBInf = read_yaml_to_dict('./conf/pcnDB_inf.yml')
    testRoutes = PCNROUTE(pcnDBInf)
    routeDict = testRoutes.get_all_routes(DataStorage=True)

    save_obj(testRoutes.pcnBussInf, 'pcnBussInf')
    save_obj(testRoutes.channelInfPd, 'channelInfPd')
    save_obj(testRoutes.routeDict, 'routeDict')
    save_obj(testRoutes.stnDict, 'stnDict')    

    print(len(routeDict.keys()))