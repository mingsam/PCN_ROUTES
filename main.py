import os 

from pcn_route import PCNROUTE
from route_limit import RoutesLimit
from route_optimun import RouteOpti
from utools.yml_io import *
from utools.pickle_io import *

def get_pcn_routes(pcnDBInf_path='./conf/pcnDB_inf.yml'):
    pcnDBInf = read_yaml_to_dict(pcnDBInf_path)
    findRoutes = PCNROUTE(pcnDBInf)
    pcnBussInf, channelInfPd, stnDict, routeDict = findRoutes.get_all_routes(DataStorage=True)

    routesLimit = RoutesLimit(pcnBussInf, channelInfPd, routeDict, findRoutes.pcnDB)
    routeLimitDict = routesLimit.get_route_limit()

    routeOpti =  RouteOpti(routeLimitDict, stnDict)
    optRoutes, routeScoreDict = routeOpti.get_optimun_routes()
    
    return optRoutes, routeScoreDict

if __name__ == '__main__':
    pcnDBInf = read_yaml_to_dict('./conf/pcnDB_inf.yml')
    findRoutes = PCNROUTE(pcnDBInf)
    pcnBussInf, channelInfPd, stnDict, routeDict = findRoutes.get_all_routes(DataStorage=True)

    routesLimit = RoutesLimit(pcnBussInf, channelInfPd, routeDict, findRoutes.pcnDB)
    routeLimitDict = routesLimit.get_route_limit()

    routeOpti =  RouteOpti(routeLimitDict, stnDict)
    optRoutes, routeScoreDict = routeOpti.get_optimun_routes()


    save_obj(optRoutes, 'optRoutes')
    save_obj(routeScoreDict, 'routeScoreDict')

    print('Obj:$\{optRoutes\} output to ./obj/optRoutes.pkl')
    print('Obj:$\{routeScoreDict\} output to ./obj/routeScoreDict.pkl')