using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cogent.DataHubAPI;
using Newtonsoft.Json;

namespace DataHubLibV1
{
    public class DataType
    {
        public string PointName { get; set; }
        public DataDetail DataDetail { get; set; }
    }
    public class DataDetail
    {
        public string PointGetDateString { get; set; }
        public string PointStrVal { get; set; }
        public string PointQuality { get; set; }
    }
    public class DataHubClass : DataHubEventConsumer
    {
        private DataHubConnector m_Connector;

        private DataType DataType = new DataType();
        private DataDetail DataDetail = new DataDetail();
        private ArrayList DataArray = new ArrayList();

        public void connect(string host, string port, string domain)
        {
            if (m_Connector == null)
            {
                m_Connector = new DataHubConnector(this);
                //m_Connector.SetISynchronizeInvoke(this);
            }
            else
            {
                m_Connector.closeConnection("Disconnect by user");
            }
            m_Connector.setConnectionParms(host, port);
            m_Connector.setDefaultDomain(domain);
            m_Connector.initializePointCache();
//          m_Connector.setHeartbeatTimes (1000, 5000);
            m_Connector.startReconnectionTimer();
            m_Connector.openConnection();
        }

        public string[] getDataHub_Data()
        {
            string[] arrString = (string[])DataArray.ToArray(typeof(string));
            return arrString;
        }

        public void updateList(DataHubPoint point)
        {
            // Console.WriteLine(point.Name + ":" + point.getDateString() + ":" + point.StrVal + ":" + point.Quality);

            for (int i = 0; i < DataArray.Count; i++)
            {
                if (JsonConvert.DeserializeObject<DataType>(DataArray[i].ToString()).PointName == point.Name)
                {
                    DataArray.RemoveAt(i);
                }
            }

            DataType.PointName = point.Name;
            DataDetail.PointGetDateString = point.getDateString();
            DataDetail.PointStrVal = point.StrVal;
            DataDetail.PointQuality = point.Quality.ToString();
            DataType.DataDetail = DataDetail;
            DataArray.Add(JsonConvert.SerializeObject(DataType));
        }


        public void onAlive()
        {
            Console.WriteLine("Alive");
        }

        public void onAsyncMessage(string[] arguments)
        {
            Console.WriteLine("onAsyncMessage");
        }

        public void onConnecting(string host, int port)
        {
            Console.WriteLine("Connecting to " + host + ":" + port);
        }

        public void onConnectionFailure(string host, int port, string reason)
        {
            Console.WriteLine("Disconnected from " + host + ":" + port + " - " + reason);
        }

        public void onConnectionSuccess(string host, int port)
        {
            Console.WriteLine("Connected to " + host + ":" + port);
            m_Connector.registerDomain(m_Connector.getDefaultDomain(), DataHubConnector.DH_REG_FUTURE | DataHubConnector.DH_REG_ALL);
        }

        public void onError(int status, string message, string cmd, string parms)
        {
            Console.WriteLine("Error: (" + cmd + " " + parms + "): " + message);
        }

        public void onPointChange(DataHubPoint point)
        {
            updateList(point);
        }

        public void onPointEcho(DataHubPoint point)
        {
            throw new NotImplementedException();
        }

        public void onStatusChange(int oldstate, int newstate)
        {
            Console.WriteLine("Status: " + m_Connector.getCnxStateString(oldstate) +
                " changed to " + m_Connector.getCnxStateString(newstate));
        }

        public void onSuccess(string cmd, string parms)
        {
            Console.WriteLine("Success: " + cmd + " " + parms);
        }

    }
}
