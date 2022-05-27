import cx_Oracle
import os
import pandas as pd

class PCNDB():
    def __init__(self, dbUser, dbPass, dbSID):
        conStr = '{}/{}@{}'.format(dbUser, dbPass, dbSID)
        self.pcnCon = cx_Oracle.connect(conStr)
        self.cursor = self.pcnCon.cursor()

    def _exec_sql(self, sql_str):
        self.cursor.execute(sql_str)
        colList = [i[0] for i in self.cursor.description]
        rowsList = self.cursor.fetchall()
        return colList, rowsList

    def _trans_dict(self, colList, rowsList):
        if len(rowsList) == 0:
            return dict(zip(colList, [[None] for i in colList]))
        assert len(colList) == len(rowsList[0])
        resDict = dict(zip(colList, [list() for i in range(len(colList))]))
        for idx, valData in enumerate(rowsList):
            for col, val in zip(colList, valData):
                resDict[col].append(val)
        return resDict

    def _del_tCols(self, tCols=[]):
        tCols_str = ''
        if len(tCols) > 0:
            for col in tCols:
                addStr = '{}, '.format(col) if col != tCols[-1] else col
                tCols_str += addStr
        else:
            tCols_str = '*'
        return tCols_str

    def get_table_all_inf(self, tName, tCols=[]):
        tCols_str = self._del_tCols(tCols)
        selectSQL = 'SELECT {} FROM {}'.format(tCols_str, tName) 
        colList, rowsList = self._exec_sql(selectSQL)
        resDict = self._trans_dict(colList, rowsList)
        return resDict

    def _def_coninf(self, conDict):
        assert len(conDict.keys()) != 0
        conStrList = []
        for col, conData in conDict.items():
            assert len(conData) == 3
            conVal, conSymbol, conDtype = conData[0], conData[1], conData[2]
            conStr = ''
            if not isinstance(conVal, list):
                conVal = list(conVal)

            if conSymbol == '=':
                assert len(conVal) == 1
                valData = conVal[0]
                valStr = '\'{}\''.format(valData) if conDtype == 'str' else valData
                conStr = '{}={}'.format(col, valStr)
            elif conSymbol == 'in':
                assert len(conVal) > 0
                valStr = ''
                for d in conVal:
                    dataStr = '\'{}\''.format(d) if conDtype == 'str' else d
                    valStr += dataStr
                    if d != conVal[-1]:
                        valStr += ', '
                conStr = '{} in ({})'.format(col, valStr)
            elif conSymbol == '[]':
                assert len(conVal) == 2
                valLStr = '\'{}\''.format(conVal[0]) if conDtype == 'str' else conVal[0]
                valRStr = '\'{}\''.format(conVal[1]) if conDtype == 'str' else conVal[1]
                conStr = '({0} >= {1} and {0} <= {2})'.format(col, valLStr, valRStr)
            elif conSymbol == '()':
                assert len(conVal) == 2
                valLStr = '\'{}\''.format(conVal[0]) if conDtype == 'str' else conVal[0]
                valRStr = '\'{}\''.format(conVal[1]) if conDtype == 'str' else conVal[1]
                conStr = '({0} > {1} and {0} < {2})'.format(col, valLStr, valRStr)

            conStrList.append(conStr)
        return conStrList

    def get_con_str_inf(self, tName, conStr, tCols=[]):
        assert isinstance(conStr, str) and conStr != ''
        tCols_str = self._del_tCols(tCols)
        selectSQL = 'SELECT {} FROM {} WHERE {}'.format(tCols_str, tName, conStr) 
        colList, rowsList = self._exec_sql(selectSQL)
        resDict = self._trans_dict(colList, rowsList)
        return resDict

    ##conDict={'$Column_Name':[[], $symbol(=, in, [], ()), $dtype(str, int, float)]}
    ##conLogicList=['and' | 'or']
    def get_con_dict_inf(self, tName, conDict, conLogicList=[], tCols=[]):
        assert len(conDict.keys()) == len(conLogicList) + 1
        tCols_str = self._del_tCols(tCols)
        tCon_str = ''
        idx_conLogic = 0
        conStrList = self._def_coninf(conDict)

        for idx_con, conStr in enumerate(conStrList):
            if idx_conLogic == len(conLogicList):
                tCon_str += conStr
            elif idx_conLogic < len(conLogicList):
                tCon_str += '{} {} '.format(conStr, conLogicList[idx_conLogic])
            idx_conLogic += 1

        if isinstance(tName, list):
            tNameStr = ''
            for t in tName:
                if t == tName[-1]:
                    tNameStr += t
                else:
                    tNameStr += '{}, '.format(t)
        else:
            tNameStr = tName
        resDict = self.get_con_str_inf(tNameStr, tCon_str, tCols)
        return resDict

    def get_station_inf(self, stnID, tCols=['OBJ_ID', 'NAME', 'FULL_NAME', 
                                            'STATION_TYPE', 'VOLTAGE_CLASS',
                                            'DSP_LEVEL', 'SITEDSP']):
        tName = 'T_SPC_SITE'
        conDict = {
            'OBJ_ID':[[stnID], '=', 'str']
        }
        res = self.get_con_dict_inf(tName=tName, conDict=conDict, tCols=tCols)
        return res

    def get_business_inf(self, b_id, tCols=['OBJ_ID', 'FULL_NAME', 'BUZ_TYPE', 
                                            'A_SITE_ID', 'Z_SITE_ID', 
                                            'DISPATCH_LEVEL', 'BUZ_RATE']):
        tName = 'T_BUZ'
        conDict = {
            'OBJ_ID':[[b_id], '=', 'str']
        }
        res = self.get_con_dict_inf(tName=tName, conDict=conDict, tCols=tCols)
        return res
        
    def get_all_businesses(self, tCols=['BUSINESS_ID', 'CHANNEL_ID']):
        tName = 'T_BUSINESS_CHANNEL'
        tCols_str = self._del_tCols(tCols)
        selectSQL = 'SELECT {} FROM {}'.format(tCols_str, tName)
        cols, rows = self._exec_sql(selectSQL)
        res = self._trans_dict(cols, rows)
        return res

    def get_channel_inf(self, c_id, tCols=['OBJ_ID', 'NAME', 'CHANNEL_TYPE', 
                                            'RATE', 'A_RES_ID', 'Z_RES_ID',
                                            'A_STATION', 'Z_STATION']):
        tName = 'T_CHANNEL_BASE'
        conDict = {
            'OBJ_ID':[[c_id], '=', 'str']
        }
        res = self.get_con_dict_inf(tName=tName, conDict=conDict, tCols=tCols)
        return res

    def get_all_channels(self, tCols=['OBJ_ID', 'NAME', 'CHANNEL_TYPE', 
                                            'RATE', 'A_RES_ID', 'Z_RES_ID',
                                            'A_STATION', 'Z_STATION']):
        tName = 'T_CHANNEL_BASE'
        tCols_str = self._del_tCols(tCols)
        selectSQL = 'SELECT {} FROM {}'.format(tCols_str, tName)
        cols, rows = self._exec_sql(selectSQL)
        res = self._trans_dict(cols, rows)
        return res

    def get_fac_inf(self, c_id, tCols=['T_SYS.NAME']):
        tName= ['T_CHANNEL_BASE, T_SYS, T_NE']
        conDict = {
            'T_CHANNEL_BASE.OBJ_ID': [[c_id], '=', 'str'],
            'T_NE.OBJ_ID': [['T_CHANNEL_BASE.A_NE'], '=', 'name'],
            'T_SYS.OBJ_ID': [['T_NE.SYS_ID'], '=', 'name']
        }
        res = self.get_con_dict_inf(tName=tName, conDict=conDict, tCols=tCols, conLogicList=['and', 'and'])
        return res
    
    def get_buss_type(self, b_id, tCols=['BUZ_TYPE']):
        tName = 'T_BUZ'
        conDict = {
            'OBJ_ID':[[b_id], '=', 'str']
        }
        res = self.get_con_dict_inf(tName=tName, conDict=conDict, tCols=tCols)
        return res
        