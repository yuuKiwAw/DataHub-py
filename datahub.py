#author:yuki

import clr
import time
import json

clr.FindAssembly("Cogent.dll")
clr.AddReference("Cogent")
from Cogent.DataHubAPI import *
clr.FindAssembly("DataHubLibV1.dll")
clr.AddReference("DataHubLibV1")
from DataHubLibV1 import *


def main():
    hostname = "localhost"
    port = "4502"
    domain = "DataPid"
    
    #! 示例话DataHubClass对象并且继承DataHubEventConsumer类
    DataHub = DataHubClass(DataHubEventConsumer)
    #! 创建连接
    DataHub.connect(hostname, port, domain)
    while 1:
        time.sleep(5)
        #? 终端清屏
        print("\033c", end="")
        #? 获取Data（返回list格式）
        DataHub_List = DataHub.getDataHub_Data()
        for item in DataHub_List:
            DataHub_Data_Dict = json.loads(item);
            print (DataHub_Data_Dict)


if __name__ == '__main__':
    main()
