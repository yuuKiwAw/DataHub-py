#author:yuki

import clr
import time
import logging
import os.path
import sys
import signal
from pydantic import BaseModel
from retrying import retry

clr.FindAssembly("Cogent.dll")
clr.AddReference("Cogent")
from Cogent.DataHubAPI import *
clr.FindAssembly("DataHubLibV1.dll")
clr.AddReference("DataHubLibV1")
from DataHubLibV1 import *


class DataHubSet(BaseModel):
    hostname: str
    port: str
    domain: str


#! logging日志模块
logger = logging.getLogger()
logger.setLevel(logging.INFO)

log_path = os.path.dirname(os.getcwd()) + '/DataHub/logs/'
file_name = 'datahub_errors_' + time.strftime('%Y-%m-%d-%H-%M', time.localtime(time.time())) + '.log'

fh = logging.FileHandler(log_path + file_name, mode='w', encoding='utf-8')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
#! logging日志模块


@retry()
def GetDataHubInfo(DataHubSet):
    """ 获取DataHub数据（带异常抛出重试）
    Args:
        DataHubSet ([type]): [description]
    """
    dhs = DataHubSet
    hostname = dhs.hostname
    port = dhs.port
    domain = dhs.domain
    try:
        #! 示例话DataHubClass对象并且继承DataHubEventConsumer类
        DataHub = DataHubClass(DataHubEventConsumer)
        #! 创建连接
        DataHub.connect(hostname, port, domain)
        logger.info('Start Connection')

        while 1:
            time.sleep(0.1)
            #? 终端清屏
            print("\033c", end="")
            print("#########################" + time.asctime() + "#########################" + "\n")
            #? 获取Data（返回list格式）
            DataHub_List = DataHub.getDataHub_Data()

            for item in range(len(DataHub_List)):
                print(DataHub_List[item])
    #! 异常处理抛出
    except Exception as ex:
        logger.error(ex)
        print(ex)
        raise Exception
    except KeyboardInterrupt:
        logger.warning('Stop manually!')
        pass


def main():
    dhs_s1 = DataHubSet
    dhs_s1.hostname = "localhost"
    dhs_s1.port = "4502"
    dhs_s1.domain = "DataPid"

    GetDataHubInfo(dhs_s1)

if __name__ == '__main__':
    main()
