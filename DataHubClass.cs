using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cogent.DataHubAPI;
using Newtonsoft.Json;

namespace DataHubLibV1
{
    public class DataHubClass : DataHubEventConsumer
    {
        private DataHubConnector m_Connector;

        private string point_name;
        private string point_getDateString;
        private string point_strVal;
        private int point_Quality;

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

        public string getDataHub_Data()
        {
            Dictionary<string, string> DataHub = new Dictionary<string, string>();
            DataHub.Add("point_name", point_name);
            DataHub.Add("point_getDateString", point_getDateString);
            DataHub.Add("point_strVal", point_strVal);
            DataHub.Add("point_Quality", point_Quality.ToString());

            string json_DataHub_str = JsonConvert.SerializeObject(DataHub);
            return json_DataHub_str;
        }

        public void updateList(DataHubPoint point)
        {
            // Console.WriteLine(point.Name + ":" + point.getDateString() + ":" + point.StrVal + ":" + point.Quality);
            point_name = point.Name;
            point_getDateString = point.getDateString();
            point_strVal = point.StrVal;
            point_Quality = point.Quality;
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
