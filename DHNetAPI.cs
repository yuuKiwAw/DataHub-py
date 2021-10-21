/*
 * (C) Copyright Cogent Real-Time Systems Inc., 1997-2005.
 *     All rights reserved.
 *
 * This file constitutes a part of the Cascade DataHub(TM) Application
 * Programming Interface.  You may use this file, at no cost to you,
 * for the purpose of creating programs that will communicate with the
 * Cascade DataHub and related products from Cogent Real-Time Systems
 * Inc., and for no other purpose.  You may modify this file in order
 * to accomplish this purpose.  Any modifications made to this file
 * become the property of Cogent Real-Time Systems Inc., and are
 * covered by this notice.
 *
 * Cogent claims no rights in any program that you create by compiling
 * this file, and accepts no responsibility or liability in such a
 * program.
 *
 * You may not sell the contents of this file, or any part thereof, or
 * attempt to gain in any way from the transmission of this file.
 *
 * You may not remove this notice from this file, nor modify it in any
 * way.
 *
 * If you wish to use this file for any purpose or under any condition
 * other than expressly allowed in this notice, you must first obtain
 * a license to do so from Cogent Real-Time Systems Inc.  Please
 * contact Cogent at info@cogent.ca.
 */

using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Text;
using System.Timers;
using System.Globalization;
using System.ComponentModel;
using System.Collections;
using System.Security.Cryptography;
using System.Collections.Generic;

// In VS2005 you can disable the "Missing XML comment" warning
// #pragma warning disable 1591

namespace Cogent
{
	namespace DataHubAPI
	{
		public class DataHubPoint
		{
			String		m_name;
			int 		m_type;
			String		m_sval;
			int			m_ival;
			double		m_fval;
			int			m_confidence;
			int			m_quality;
			int			m_locked;
			int			m_security;
			int			m_seconds;
			int			m_nanoseconds;
			int			m_flags;
			Object		m_userdata;
	
			public const int    PT_TYPE_STRING	= 0;
			public const int    PT_TYPE_REAL	= 1;
			public const int    PT_TYPE_INT32	= 2;
			public const int    PT_TYPE_VOID	= 3;

			public DataHubPoint (String sname)
			{
				m_name = sname;
				m_type = PT_TYPE_INT32;
				m_ival = 0;
				setInfo (0, PT_QUALITY_BAD, 0, 0, 0, 0, 0);
				setTimeStamp();
			}

			public DataHubPoint (String sname, int itype, String svalue, int iconf, int iquality,
				int isecurity, int ilocked, int iseconds, int inanoseconds, int flags)
			{
				m_name = sname;
				m_userdata = null;
				setInfo (iconf, iquality, isecurity, ilocked, iseconds, 
					inanoseconds, flags);
				setValueFromString (svalue, itype);
			}
	
			public void clear()
			{
				m_name = null;
				m_type = PT_TYPE_INT32;
				m_ival = 0;
				m_sval = null;
				m_fval = 0;
				m_confidence = 0;
				m_quality = PT_QUALITY_BAD;
				m_locked = 0;
				m_security = 0;
				m_seconds = 0;
				m_nanoseconds = 0;
				m_flags = 0;
				m_userdata = null;
			}

			public void copy (DataHubPoint src)
			{
				m_name = src.m_name;
				m_type = src.m_type;
				m_ival = src.m_ival;
				m_sval = src.m_sval;
				m_fval = src.m_fval;
				m_confidence = src.m_confidence;
				m_quality = src.m_quality & PT_STATUS_MASK;
				m_locked = src.m_locked;
				m_security = src.m_security;
				m_seconds = src.m_seconds;
				m_nanoseconds = src.m_nanoseconds;
				m_flags = src.m_flags;
				m_userdata = src.m_userdata;
			}
	
			public void setInfo (int iconf, int iquality, int isecurity, int ilocked,
				int iseconds, int inanoseconds, int flags)
			{
				m_confidence = iconf;
                m_quality = iquality & PT_STATUS_MASK;
				m_locked = ilocked;
				m_security = isecurity;
				m_seconds = iseconds;
				m_nanoseconds = inanoseconds;
				m_flags = flags;
			}

			public void setInfo (DataHubPoint point)
			{
				m_confidence = point.m_confidence;
				m_locked = point.m_locked;
				m_nanoseconds = point.m_nanoseconds;
                m_quality = point.m_quality & PT_STATUS_MASK;
				m_seconds = point.m_seconds;
				m_security =  point.m_security;
				m_flags = point.m_flags;
			}
	
			public String getStringValue ()
			{
				switch (m_type)
				{
					case 0:
						return (m_sval);
					case 1:
						return (m_fval.ToString(CultureInfo.InvariantCulture.NumberFormat));
					default:
						return (m_ival.ToString(CultureInfo.InvariantCulture.NumberFormat));
				}
			}
	
			public double getDoubleValue()
			{
				switch (m_type)
				{
					case 0:
						return (double.Parse(m_sval, CultureInfo.InvariantCulture.NumberFormat));
					case 1:
						return (m_fval);
					default:
						return ((double)m_ival);
				}
			}

			public int getIntValue()
			{
				switch (m_type)
				{
					case 0:
						return (int.Parse(m_sval, CultureInfo.InvariantCulture.NumberFormat));
					case 1:
						return ((int)m_fval);
					default:
						return (m_ival);
				}
			}

			public void setValueFromString (String value, int type)
			{
				try
				{
					m_type = type;
					if (type == PT_TYPE_STRING)
						m_sval = value;
					else if (type == PT_TYPE_REAL)
						m_fval = double.Parse(value, CultureInfo.InvariantCulture.NumberFormat);
					else if (type == PT_TYPE_INT32)
						m_ival = int.Parse(value, CultureInfo.InvariantCulture.NumberFormat);
					else
					{
						m_sval = value;
						type = PT_TYPE_STRING;
					}
				}
				catch
				{
					m_sval = "";
					m_ival = 0;
					m_fval = 0.0;
				}
			}

			public void setValueFromString (String value)
			{
				try
				{
					m_type = 2;
					m_ival = int.Parse(value, CultureInfo.InvariantCulture.NumberFormat);
				}
				catch
				{
					try
					{
						m_type = 1;
						m_fval = double.Parse(value, CultureInfo.InvariantCulture.NumberFormat);
					}
					catch
					{
						m_type = 0;
						m_sval = value;
					}
				}
			}

			public const double	DAYS_TO_UNIX_TIME =			25569.0;
			public const double	SECS_PER_DAY =				(60 * 60 * 24);

			private double dh_UnixTimeToWindowsTime (int secs, int nsecs)
			{
                return UnixTimeToWindowsTime(secs, nsecs);
			}

            static public double UnixTimeToWindowsTime(int secs, int nsecs)
            {
                double dsecs = ((double)secs + (double)nsecs / 1.0e9);
                return UnixTimeToWindowsTime(dsecs);
            }

            static public double UnixTimeToWindowsTime(double secs)
            {
                double days = secs / SECS_PER_DAY;
                double date = DAYS_TO_UNIX_TIME + days;
                return (date);
            }

            static public double WindowsTimeToUnixTime(double oadate)
            {
                if (oadate < DAYS_TO_UNIX_TIME)
                    return (-1);
                oadate -= DAYS_TO_UNIX_TIME;
                oadate *= SECS_PER_DAY;
                return oadate;
            }
	        
            static public bool WindowsTimeToUnixTime(double oadate, out int secs, out int nsecs)
            {
                double unix = WindowsTimeToUnixTime(oadate);
                if (unix >= 0)
                {
                    secs = (int)(Math.Floor(unix));
                    nsecs = (int)((unix - secs) * 1.0e9);
                    return true;
                }
                else
                {
                    secs = nsecs = 0;
                    return false;
                }
            }

			public void setTimeStamp (int seconds, int nanoseconds)
			{
				m_seconds = seconds;
				m_nanoseconds = nanoseconds;
			}

			public void setTimeStamp (DateTime datetime)
			{
				double			date = datetime.ToOADate();
                int             secs, nsecs;

                if (WindowsTimeToUnixTime(date, out secs, out nsecs))
                {
                    setTimeStamp(secs, nsecs);
                }
			}

			public void setTimeStamp ()
			{
				setTimeStamp (DateTime.UtcNow);
			}

			public const int    PT_QUALITY_MASK =           0xC0;
			public const int    PT_STATUS_MASK =            0xFC;
			public const int    PT_LIMIT_MASK =             0x03;

			public const int    PT_QUALITY_BAD =            0x00;
			public const int    PT_QUALITY_UNCERTAIN =      0x40;
			public const int    PT_QUALITY_GOOD =           0xC0;
			public const int    PT_QUALITY_CONFIG_ERROR =   0x04;
			public const int    PT_QUALITY_NOT_CONNECTED =  0x08;
			public const int    PT_QUALITY_DEVICE_FAILURE = 0x0c;
			public const int    PT_QUALITY_SENSOR_FAILURE = 0x10;
			public const int    PT_QUALITY_LAST_KNOWN =     0x14;
			public const int    PT_QUALITY_COMM_FAILURE =   0x18;
			public const int    PT_QUALITY_OUT_OF_SERVICE = 0x1C;
			public const int    PT_QUALITY_LAST_USABLE =    0x44;
			public const int    PT_QUALITY_SENSOR_CAL =     0x50;
			public const int    PT_QUALITY_EGU_EXCEEDED =   0x54;
			public const int    PT_QUALITY_SUB_NORMAL =     0x58;
			public const int    PT_QUALITY_LOCAL_OVERRIDE = 0xD8;

			public String getQualityString ()
			{
				String		str;
				switch (m_quality)
				{
					case PT_QUALITY_BAD:
						str = "Bad";
						break;
					case PT_QUALITY_UNCERTAIN:
						str = "Uncertain";
						break;
					case PT_QUALITY_GOOD:
						str = "Good";
						break;
					case PT_QUALITY_CONFIG_ERROR:
						str = "Config error";
						break;
					case PT_QUALITY_NOT_CONNECTED:
						str = "Not connected";
						break;
					case PT_QUALITY_DEVICE_FAILURE:
						str = "Device failure";
						break;
					case PT_QUALITY_SENSOR_FAILURE:
						str = "Sensor failure";
						break;
					case PT_QUALITY_LAST_KNOWN:
						str = "Last known";
						break;
					case PT_QUALITY_COMM_FAILURE:
						str = "Comm failure";
						break;
					case PT_QUALITY_OUT_OF_SERVICE:
						str = "Out of service";
						break;
					case PT_QUALITY_LAST_USABLE:
						str = "Last usable";
						break;
					case PT_QUALITY_SENSOR_CAL:
						str = "Sensor calibration";
						break;
					case PT_QUALITY_EGU_EXCEEDED:
						str = "EGU exceeded";
						break;
					case PT_QUALITY_SUB_NORMAL:
						str = "Subnormal";
						break;
					case PT_QUALITY_LOCAL_OVERRIDE:
						str = "Local override";
						break;
					default:
						str = "Unknown";
						break;
				}
				return (str);
			}

			public String getDateString ()
			{
				double		mydate = dh_UnixTimeToWindowsTime (Seconds, Nanoseconds);
				DateTime	date = DateTime.FromOADate (mydate);
				date = date.ToLocalTime();
				double ms = Nanoseconds/1000000;
				return (date.ToString("MMM dd HH:mm:ss") + "." + ms.ToString("000"));
			}

            public double getOADate()
            {
                double mydate = dh_UnixTimeToWindowsTime(Seconds, Nanoseconds);
                return mydate;
            }

            public DateTime getDateTime()
            {
                double mydate = dh_UnixTimeToWindowsTime(Seconds, Nanoseconds);
                return DateTime.FromOADate(mydate);
            }

			public static String unqualifyName (String pointname)
			{
				int		indx = pointname.IndexOf(':');
				if (indx != -1)
					pointname = pointname.Substring(indx+1);
				return (pointname);
			}

			public static String qualifyName (String domainname, String pointname)
			{
				return (domainname + ":" + unqualifyName(pointname));
			}

			// Accessors, for compatibility with Java and C++
			public void setName (String name) { m_name = name; }
			public void setName (String sdomain, String sname) { m_name = sdomain + ":" + sname; }
			public void setConfidence (int confidence) { m_confidence = confidence; }
            public void setQuality(int quality) { m_quality = quality & PT_STATUS_MASK; }
			public void setLocked (int locked) { m_locked = locked; }
			public void setSecurity (int security) { m_security = security; }
			public void setFlags (int flags) { m_flags = flags; }
			public void setValue (int value) { m_type = PT_TYPE_INT32; m_ival = value; }
			public void setValue (double value) { m_type = PT_TYPE_REAL; m_fval = value; }
			public void setValue (String value) { m_type = PT_TYPE_STRING; m_sval = value; }
			public void setValue(DataHubPoint point)
			{
				m_sval = point.m_sval;
				m_ival = point.m_ival;
				m_fval = point.m_fval;
				m_type = point.m_type;
			}
			public void setUserdata (Object value) { m_userdata = value; }

			public String getName () { return m_name; }
			public int getType () { return m_type; }
			public int getConfidence () { return m_confidence; }
			public int getQuality () { return m_quality; }
			public int getLocked () { return m_locked; }
			public int getSecurity () { return m_security; }
			public int getSeconds () { return m_seconds; }
			public int getNanoseconds () { return m_nanoseconds; }
			public int getFlags () { return m_flags; }
			public Object getUserdata() { return m_userdata; }

			// Properties
			public String Name { get { return m_name; } set { m_name = value; } }
			public int Type { get { return m_type; } set { m_type = value; } }
			public int Confidence { get { return m_confidence; } set { m_confidence = value; } }
            public int Quality { get { return m_quality; } set { m_quality = value & PT_STATUS_MASK; } }
			public int Locked { get { return m_locked; } set { m_locked = value; } }
			public int Security { get { return m_security; } set { m_security = value; } }
			public int Seconds { get { return m_seconds; } set { m_seconds = value; } }
			public int Nanoseconds { get { return m_nanoseconds; } set { m_nanoseconds = value; } }
			public int Flags { get { return m_flags; } set { m_flags = value; } }
			public Object Userdata { get { return m_userdata; } set { m_userdata = value; } }
			public int IntVal { get { return getIntValue(); } set { m_type = PT_TYPE_INT32; m_ival = value; } }
			public double DblVal { get { return getDoubleValue(); } set { m_type = PT_TYPE_REAL; m_fval = value; } }
			public String StrVal { get { return getStringValue(); } set { m_type = PT_TYPE_STRING; m_sval = value; } }
		}

		public interface DataHubEventConsumer
		{
			void onPointChange (DataHubPoint point);
			void onPointEcho (DataHubPoint point);
			void onAsyncMessage (String[] arguments);
			void onAlive ();
			void onSuccess (String cmd, String parms);
			void onError (int status, String message, String cmd, String parms);
			void onConnectionSuccess (String host, int port);
			void onConnectionFailure (String host, int port, String reason);
			void onConnecting (String host, int port);
			void onStatusChange (int oldstate, int newstate);
		}

		/// <summary>
		/// This is the TCP Connector class used to initiate and maintain a Cascade
		/// DataHub connection, and to dispatch events from that connection.
		/// </summary>
		public class DataHubConnector
		{
			String							m_hostname;
			String							m_portstring;
			String							m_domain;
			String							m_username;
			String							m_password;
			//		TcpClient						m_TcpClient;
			Socket							m_socket;
			int								m_port;

			byte[]							m_buffer;
			int								m_buflen = 0;
			int								m_offset = 0;
			int								m_offsetprev = 0;

			DataHubEventConsumer			m_target;
			bool							m_wantsuccess = false;

			// Ideally we want to process incoming messages in the background thread
			// where they are originally received.	This is much more responsive, so
			// long as all calls to callback functions go through delegates so they
			// are handled in the user's thread.  The problem with this approach is
			// that there could be races on the point structures.
			bool m_processintimer = false;

			// If the socket is currently disconnected, we will try every m_reconnectdelay
			// milliseconds through the m_retrytimer.
			System.Timers.Timer				m_retrytimer;
			// While the socket is alive we send and receive (alive) messages as necessary
			// at the heartbeat rate.  If we get no data within the timeout time, disconnect
			// the connection.  The retrytimer will try again later if it is active.
			System.Timers.Timer				m_alivetimer;
			// In order to do the connection processing in the main thread we use a timer
			// that periodically looks for state changes in the connection state.  This is
			// not a great approach, but it keeps thread synchronization simple.
			System.Timers.Timer				m_connecttimer;

			ISynchronizeInvoke				m_ISynchronizeInvoke;

			Hashtable						m_pointcache;

			int								m_heartbeat;		// in milliseconds
			int								m_timeout;			// in milliseconds
			int								m_reconnectdelay;	// in milliseconds
			int								m_idletime;			// in milliseconds
			bool							m_outputsent;		// we have transmitted something recently
			int								m_connstate;
			int								m_connstateprev;
			String							m_errmsg;

            System.Threading.Mutex          m_TickMutex;        // Mutex to protect the status tick timer handler

			public static ManualResetEvent	m_connectdone = new ManualResetEvent (false);
			public static ManualResetEvent	m_senddone = new ManualResetEvent(false);
			public static ManualResetEvent	m_receivedone = new ManualResetEvent(false);


			public const int DHC_STATE_NONE						=0;
			public const int DHC_STATE_IDLE						=0x0001;
			public const int DHC_STATE_INITIALIZED				=0x0002;
			public const int DHC_STATE_CONNECTING				=0x00F0;
			public const int DHC_STATE_CONN_RESOLVE_PORT_NAME	=0x0010;
			public const int DHC_STATE_CONN_ERR_PORT_FAIL		=0x1010;
			public const int DHC_STATE_CONN_RESOLVE_HOST_NAME	=0x0020;
			public const int DHC_STATE_CONN_ERR_HOST_FAIL		=0x1020;
			public const int DHC_STATE_CONN_ALLOCATE			=0x0030;
			public const int DHC_STATE_CONN_ERR_ALLOC_FAIL		=0x1030;
			public const int DHC_STATE_CONN_CONNECT				=0x0040;
			public const int DHC_STATE_CONN_ERR_CONNECT_FAIL	=0x1040;
			public const int DHC_STATE_CONN_ERR_SELECT_FAIL		=0x1050;
			public const int DHC_STATE_CONNECTED				=0x0100;
			public const int DHC_STATE_RETRY_DELAY				=0x0200;
			public const int DHC_STATE_TERMINATING				=0x0004;

			public DataHubConnector(DataHubEventConsumer target)
			{
				m_target = target;
				m_reconnectdelay = 5000;
				m_heartbeat = 0;
				m_timeout = 0;
				m_idletime = 0;
				m_outputsent = false;
				m_pointcache = new Hashtable();
				m_connstate = m_connstateprev = DHC_STATE_IDLE;

				/* This event watches to see if the connection state of the connector
				 * has transitioned into DHC_STATE_CONNECTED.
				 * When it does, we do all of the rest of the connection handling, but
				 * in the main thread, not the async I/O thread.
				 */
				ElapsedEventHandler evhandler = new ElapsedEventHandler(ConnectTimerHandler);
				m_connecttimer = new System.Timers.Timer(10);
				m_connecttimer.AutoReset = true;
				m_connecttimer.Elapsed += evhandler;
				m_connecttimer.Start();

				/* Ideally we want to run the callbacks in the thread that created
				 * the DataHubConnector object.	 If the target implements
				 * ISynchronizeInvoke then we can do that.	If not, we will end up
				 * running either in the thread of the socket receiver if
				 * m_processintimer == true, or in the thread of a timer if
				 * m_processintimer == false.  Neither is very satisfactory, so
				 * it is better if the target implements ISynchronizeInvoke.  All
				 * objects derived from Control implement this automatically, so a Form
				 * will work.
				 */
				if (target is ISynchronizeInvoke)
				{
					m_ISynchronizeInvoke = target as ISynchronizeInvoke;
					m_processintimer = false;
				}
				else
				{
					m_processintimer = false;
				}
                m_TickMutex = new Mutex(false);
			}

            ~DataHubConnector()
            {
                shutdown("Object destroyed");
                m_TickMutex.Close();
            }

			private void ConnectTimerHandler(object sender, ElapsedEventArgs e)
			{
                if (m_TickMutex.WaitOne(0))
                {
                    try
                    {
                        lock (this)
                        {
                            int newstate = -1;

                            if (m_connstate == DHC_STATE_CONNECTED &&
                                m_connstateprev != DHC_STATE_CONNECTED)
                            {
                                try
                                {
                                    m_socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.NoDelay, 1);
                                }
                                catch (Exception)
                                {
                                    // In MONO, the above generates an exception.  Just ignore it.
                                }
                                sendWantSuccess();
                                sendLogin();
                                setDefaultDomain(m_domain);
                                sendHeartbeatTimes();
                                startHeartbeatTimers();
                                //onStatusChange(m_connstateprev, m_connstate);
                                onConnectionSuccess(m_hostname, m_port);
                                // We need to kick-start the receive cycle when we connect
                                Receive();
                            }
                            else if (m_connstate == DHC_STATE_IDLE &&
                                m_connstateprev != DHC_STATE_IDLE)
                            {
                                //onStatusChange(m_connstateprev, m_connstate);
                                onConnectionFailure(m_hostname, m_port, m_errmsg);
                            }
                            else if ((m_connstate & 0x1000) != 0)
                            {
                                //onStatusChange(m_connstateprev, m_connstate);
                                //m_connstate = DHC_STATE_IDLE;
                                newstate = DHC_STATE_IDLE;
                            }

                            if (m_connstate == DHC_STATE_CONNECTED &&
                                m_offset != m_offsetprev)
                            {
                                if (m_processintimer)
                                {
                                    ProcessMessageBuffer();
                                    m_offsetprev = m_offset;
                                    // Client could close the connection in response to a message.
                                    if (isConnected())
                                    {
                                        Receive(m_socket);
                                    }
                                }
                            }
                            m_connstateprev = m_connstate;
                            if (newstate != -1)
                            {
                                setConnState(newstate);
                            }
                        }
                    }
                    finally
                    {
                        m_TickMutex.ReleaseMutex();
                    }
                }
                else
                {
                    // Just a breakpoint target
                    m_TickMutex = m_TickMutex;
                }
			}

			public void shutdown (String reason)
			{
				lock (this)
				{
					cancelReconnectionTimer();
                    if (m_connecttimer != null)
                    {
                        m_connecttimer.Stop();
                        m_connecttimer = null;
                    }
					if (m_alivetimer != null)
					{
						m_alivetimer.Stop();
						m_alivetimer = null;
					}
					setConnState (DHC_STATE_TERMINATING);
					closeConnection (reason == null ? "Shutting down" : reason);
				}
			}

			public String getDefaultDomain()
			{
				return (m_domain);
			}

			public bool initializePointCache ()
			{
				m_pointcache = new Hashtable(64);
				return (true);
			}

			public Hashtable getPointCache()
			{
				return m_pointcache;
			}

			public DataHubPoint lookupPoint (String pointname)
			{
				DataHubPoint	point = null;
				if (m_pointcache != null)
				{
					lock(m_pointcache)
					{
						point = (DataHubPoint)m_pointcache[pointname];
						if (point == null)
						{
							point = new DataHubPoint (pointname, DataHubPoint.PT_TYPE_INT32, "0", 0,
								DataHubPoint.PT_QUALITY_BAD, 0, 0, 0, 0, 0);
							m_pointcache.Add (pointname, point);
						}
					}
				}
				return (point);
			}

			public void setConnectionParms (String hostname, String port)
			{
				setConnectionParms (hostname, int.Parse(port, CultureInfo.InvariantCulture.NumberFormat));
			}

			public void setConnectionParms (String hostname, int port)
			{
				closeConnection("Host name reset");
				m_hostname = hostname;
				m_portstring = port.ToString(CultureInfo.InvariantCulture.NumberFormat);
				m_port = port;
			}

			public void setConnectionParms (String hostname, int port, String username, String password)
			{
				setConnectionParms(hostname, port);
				m_username = username;
				m_password = password;
			}

			public void setUsername (String username, String password)
			{
				closeConnection("User name reset");
				m_username = username;
				m_password = password;
			}

			/// <summary>
			/// Start the connection process, asynchronously.
			/// </summary>
			/// <param name="remoteEP">The server specification</param>
			/// <param name="client">The Socket to connect</param>
			private static void Connect(EndPoint remoteEP, DataHubConnector client) 
			{
				try
				{
                    if (false)
                    {
                        // This code will force the connection to use localhost:4502 as the local side
                        // of the socket.  If the destination is also localhost:4502 then this will
                        // result in an "accidental" self-connect.  That lets us test the code in
                        // ConnectCallback that traps this odd error.
                        IPEndPoint local = new IPEndPoint(new IPAddress(0x0100007f), 4502);
                        client.m_socket.Bind(local);
                    }
					IAsyncResult result = client.m_socket.BeginConnect(remoteEP, 
						new AsyncCallback(ConnectCallback), client);
                    if (result.CompletedSynchronously)
                    {
                        ConnectCallback(result);
                    }
                    else if (result.IsCompleted)
                    {
                        client.m_errmsg = "Socket connection completed without connecting";
                        client.setConnState(DHC_STATE_CONN_ERR_CONNECT_FAIL);
                    }
				}
				catch (Exception e)
				{
                    client.m_errmsg = e.Message;
                    client.setConnState(DHC_STATE_CONN_ERR_CONNECT_FAIL);
				}
			}

			/// <summary>
			/// The callback function that is called when an async connection
			/// completes.
			/// </summary>
			/// <param name="ar">The result of the connection attempt</param>
			private static void ConnectCallback(IAsyncResult ar) 
			{
				// Retrieve the socket from the state object.
				DataHubConnector client = (DataHubConnector) ar.AsyncState;

				// Signal that the connection has been made.
				m_connectdone.Set();
				if (client.m_connstate != DHC_STATE_TERMINATING)
				{
					try 
					{
						// Complete the connection.  This will throw an exception if no connection
						// was made.
                        if (!ar.CompletedSynchronously)
						    client.m_socket.EndConnect(ar);

						// Generate either a connect or disconnect callback, but not here.  We
						// just mark the connection state and a timer in the main thread will
						// note the change and call the callback.
						if (client.m_socket.Connected)
						{
                            // Windows can allow a TCP connection to accidentally connect to itself.  It happens
                            // if there is no server listening on the destination port, and the random (ephemeral)
                            // source port assigned by the TCP implementation happens to match the destination port.
                            // Windows does not check for this, even though it's a real issue:
                            // http://stackoverflow.com/questions/5139808/tcp-simultaneous-open-and-self-connect-prevention

                            IPEndPoint local = client.m_socket.LocalEndPoint as IPEndPoint;
                            IPEndPoint remote = client.m_socket.RemoteEndPoint as IPEndPoint;

                            client.m_socket.NoDelay = true;

                            if (local.Port == remote.Port &&
                                local.Address.ToString().CompareTo(remote.Address.ToString()) == 0)
                            {
                                // This is an accidental self-connect.  We need to abandon the
                                // connection and try again.
                                client.m_socket.Close();
                                client.m_errmsg = "Accidental self-connect.  Retrying.";
                                client.setConnState(DHC_STATE_CONN_ERR_CONNECT_FAIL);
                            }
                            else
                            {
                                client.setConnState(DHC_STATE_CONNECTED);
                            }
                        }
						else
						{
                            client.m_errmsg = ar.ToString();
                            client.setConnState(DHC_STATE_CONN_ERR_CONNECT_FAIL);
						}
					} 
					catch (Exception e)
					{
                        client.m_errmsg = e.Message;
                        client.setConnState(DHC_STATE_CONN_ERR_CONNECT_FAIL);
					}
				}
			}

			public void closeConnection (String reason)
			{
				lock (this)
				{
					m_offset = 0;
					if (m_socket != null)
					{
						m_socket.Close ();
						cancelHeartbeatTimers();
						m_socket = null;
						// If we are not shutting down, reset to not-connected state
                        if (m_connstate != DHC_STATE_TERMINATING)
                        {
                            m_errmsg = reason;
                            setConnState(DHC_STATE_CONN_ERR_CONNECT_FAIL);
                        }
                        else
                        {
                            onConnectionFailure(m_hostname, m_port, reason);
                        }
					}
				}
			}

			/// <summary>
			/// Send data asynchronously on a socket.  We do not add a
			/// newline onto the end of the outgoing string.
			/// </summary>
			/// <param name="conn">The socket connection to a server.</param>
			/// <param name="data">The string data to send.</param>
			private static Exception Send(DataHubConnector conn, String data) 
			{
				Exception	result = null;
				Socket		client = conn.m_socket;
				try
				{
					// Convert the string data to byte data using UTF8 encoding.
					byte[] byteData = Encoding.UTF8.GetBytes(data);

					// Begin sending the data to the remote device.
					client.BeginSend(byteData, 0, byteData.Length, SocketFlags.None,
						new AsyncCallback(SendCallback), conn);

					//				m_senddone.WaitOne();
				}
				catch (Exception e)
				{
					result = e;
				}
				return (result);
			}

			/// <summary>
			/// The function used to send an arbitrary text string to the DataHub.  A 
			/// complete message is terminated by a newline character (\n), which must
			/// be added by the user.
			/// </summary>
			/// <param name="data">The message to transmit to the DataHub</param>
			/// <returns></returns>
			private Exception Send (String data)
			{
				Exception	result = null;
				if (m_connstate == DHC_STATE_CONNECTED)
				{
					result = Send (this, data);
					if (result == null)
						m_outputsent = true;
				}
				else
				{
					result = new Exception("Socket is not connected");
				}
				return (result);
			}

			/// <summary>
			/// Callback indicating that a Send has completed.
			/// </summary>
			/// <param name="ar">The result of the send.</param>
			private static void SendCallback(IAsyncResult ar) 
			{
				DataHubConnector conn = (DataHubConnector) ar.AsyncState;
				try 
				{
					// Retrieve the socket from the state object.

					lock (conn)
					{
						Socket client = conn.m_socket;

						if (client != null && conn.m_connstate != DHC_STATE_TERMINATING)
						{
							// Complete sending the data to the remote device.
							int bytesSent = client.EndSend(ar);
						}

						// Signal that all bytes have been sent.
						m_senddone.Set();
					}
				} 
				catch (Exception e) 
				{
					Console.WriteLine(e.ToString());
				}
			}

			private Exception Receive (Socket client)
			{
				Exception		result = null;
				if (m_buffer == null)
				{
					m_buflen = 1024;
					m_buffer = new byte[m_buflen];
				}
				if (m_offset == m_buflen)
				{
					byte[]	newbuf = new byte[m_buflen * 2];
					Array.Copy (m_buffer, newbuf, m_offset);
					m_buffer = newbuf;
					m_buflen *= 2;
				}
				try
				{
					client.BeginReceive (m_buffer, m_offset, m_buflen - m_offset, 0,
						new AsyncCallback(ReceiveCallback), this);
				}
				catch (Exception e)
				{
					result = e;
				}
				return (result);
			}

			private Exception Receive ()
			{
				Exception result;
				if (m_connstate == DHC_STATE_CONNECTED)
				{
					result = Receive (m_socket);
				}
				else
				{
					result = new Exception ("Socket is not connected");
				}
				return (result);
			}

			private static void ReceiveCallback(IAsyncResult ar) 
			{
				DataHubConnector conn = (DataHubConnector) ar.AsyncState;
				try 
				{
					// Signal that all bytes have been sent.
					m_receivedone.Set();

					// Retrieve the socket from the state object.
					lock (conn)
					{
						Socket client = conn.m_socket;

						if (client != null)
						{
							// Complete sending the data to the remote device.
							int nbytes = client.EndReceive (ar);

							if (conn.m_connstate != DHC_STATE_TERMINATING)
							{
								// If we receive zero bytes, this indicates that the socket
								// has closed.
								if (nbytes <= 0)
								{
									conn.closeConnection("Connection has terminated");
								}
								else
								{
									conn.m_offset += nbytes;

									// We have incoming activity on the socket
									conn.m_idletime = 0;

									if (!conn.m_processintimer)
									{
										// This processes the messages in the thread, or passes it to a delegate
										// Process the incoming message
										conn.ProcessMessageBuffer();

										// Start the next read
										conn.Receive(client);
									}
								}
							}
						}
						else
						{
							Console.WriteLine ("Receive callback on closed socket.");
						}
					}
				} 
				catch (Exception e) 
				{
					// We get an exception during a receive when the .Net framework accidentally
					// delivers an connection request response to the receive response handler.
					// Assume that the connection has died.
					Console.WriteLine(e.ToString());
                    conn.setConnState(DHC_STATE_CONN_ERR_CONNECT_FAIL);
					//conn.m_connstate = DHC_STATE_CONN_ERR_CONNECT_FAIL;
				}
			}

			private String StringConcat (String[] args, int start, String separator)
			{
				String		result=null;
				for (int i=start; i<args.Length; i++)
				{
					if (result == null)
						result = "";
					result += args[i];
					if (i < args.Length - 1)
						result += separator;
				}
				return (result);
			}

			private void ProcessMessage(String[] args)
			{
				if (m_ISynchronizeInvoke != null && m_ISynchronizeInvoke.InvokeRequired)
				{
					ProcessMessageDelegate d = new ProcessMessageDelegate(RealProcessMessage);
					m_ISynchronizeInvoke.BeginInvoke(d, new object[] { args });
				}
				else
				{
					RealProcessMessage(args);
				}
			}

			private void RealProcessMessage(String[] args)
			{
				try
				{
					DataHubPoint		point;

					if (args[0].CompareTo("error") == 0)
					{
						char[] splits = { ':', ' ', '(', ')' };
						String[] msgargs = args[1].Split(':');
						int status = int.Parse (msgargs[0], CultureInfo.InvariantCulture.NumberFormat);
						String message = msgargs[2].TrimStart(' ');
						String[] cmdargs = msgargs[1].Split(splits);
						int		count=0;
						for (int ii=0; ii < cmdargs.Length; ii++)
						{
							if (cmdargs[ii].CompareTo("") != 0)
								cmdargs[count++] = cmdargs[ii];
						}

						String cmd = cmdargs[0];
						String parms = StringConcat (cmdargs, 1, " ");
						onError (status, message, cmd, parms);
					}
					else if (args[0].CompareTo("success") == 0)
					{
						// we need to re-concatenate the arguments
						onSuccess (args[1], StringConcat(args, 2, " "));
					}
					else if (args[0].CompareTo("point") == 0 ||
						args[0].CompareTo("echo") == 0)
					{
						/* If we are using the point cache, we want to preserve the userdata
						 * by returning the same point on each call.  We just update the point
						 * information, leaving everything else about the cached point intact.
						 */

						if (m_pointcache != null)
						{
							point = lookupPoint (args[1]);
							point.setInfo (int.Parse(args[4], CultureInfo.InvariantCulture.NumberFormat),
								int.Parse(args[10], CultureInfo.InvariantCulture.NumberFormat),
								int.Parse(args[5], CultureInfo.InvariantCulture.NumberFormat),
								int.Parse(args[6], CultureInfo.InvariantCulture.NumberFormat),
								int.Parse(args[7], CultureInfo.InvariantCulture.NumberFormat),
								int.Parse(args[8], CultureInfo.InvariantCulture.NumberFormat),
								int.Parse(args[9], CultureInfo.InvariantCulture.NumberFormat));
							point.setValueFromString (args[3], int.Parse(args[2], CultureInfo.InvariantCulture.NumberFormat));
						}
						else
						{
							point = new DataHubPoint (args[1], 
								int.Parse(args[2], CultureInfo.InvariantCulture.NumberFormat),
								args[3],
								int.Parse(args[4], CultureInfo.InvariantCulture.NumberFormat),
								int.Parse(args[10], CultureInfo.InvariantCulture.NumberFormat),
								int.Parse(args[5], CultureInfo.InvariantCulture.NumberFormat),
								int.Parse(args[6], CultureInfo.InvariantCulture.NumberFormat),
								int.Parse(args[7], CultureInfo.InvariantCulture.NumberFormat),
								int.Parse(args[8], CultureInfo.InvariantCulture.NumberFormat),
								int.Parse(args[9], CultureInfo.InvariantCulture.NumberFormat));
						}
						if (args[0].CompareTo("point") == 0)
							onPointChange (point);
						else
							onPointEcho (point);
					}
					else if (args[0].CompareTo("alive") == 0)
						onAlive();
					else
						onAsyncMessage (args);
				}
				catch (Exception e)
				{
					Console.WriteLine (e.ToString());
				}
			}

            private int ProcessEachExpression(byte[] buf, int buflen)
            {
                byte nl = (byte)'\n';
                //int pos = 0;
                int maxlen = buflen;
                int offset = 0;
                int lastnl = Array.LastIndexOf(buf, nl, buflen-1);

                if (lastnl >= 0)
                {
                    String s = Encoding.UTF8.GetString(buf, 0, lastnl);
                    offset = lastnl + 1;    // we return the point after the last consumed newline
                    String[] messages = s.Split('\n');
                    foreach (String message in messages)
                    {
                        String expression = message;
                        do
                        {
                            String[] args = LispParse(expression, out expression);
                            if (args.Length > 0)
                            {
                                ProcessMessage(args);
                            }
                        } while (!String.IsNullOrEmpty(expression));
                    }
                }
                return offset;
            }

            protected void ProcessMessageBuffer()
            {
                int pos = -1;
                pos = ProcessEachExpression(m_buffer, m_offset);
                Array.Copy(m_buffer, pos, m_buffer, 0, m_offset - pos);
                m_offset -= pos;
            }

            /*
			private void xxProcessMessageBuffer ()
			{
				//			String	tmp = Encoding.UTF8.GetString (m_buffer,0,m_offset);
				byte	nl = (byte)'\n';
				int		pos = -1;

				do
				{
					pos = Array.IndexOf (m_buffer, nl, 0, m_offset);
					if (pos >= 0)
					{
						int[]		firstchar = new int[1];
						firstchar[0] = 0;
						while (firstchar[0] != -1)
						{
							String[]		args;
							int				nargs=0;

							args = LispParse (m_buffer, pos, firstchar);
							while (args[nargs] != null) nargs++;
							String[] minargs = new String[nargs];

							if (nargs > 0)
							{
								Array.Copy (args, minargs, nargs);
								ProcessMessage (minargs);
							}
							else
							{
								//							Console.WriteLine ("Zero-length input\n");
							}
						}
						//					String		result = Encoding.UTF8.GetString (m_buffer,0,pos);
                        // A user program could disconnect in response to a message, in which
                        // case we just stop processing.
                        if (isConnected())
                        {
                            m_offset -= pos + 1;
                            Array.Copy(m_buffer, pos + 1, m_buffer, 0, m_offset);
                        }
                        else
                        {
                            break;
                        }
					}
				} while (pos >= 0);
			}
            */

			/// <summary>
			/// Initiate the connection to the DataHub.
			/// </summary>
			/// <returns>int - an error number, or 0 for success</returns>
			public int openConnection ()
			{
				int				result = 0;
				IPAddress		addr;
				IPEndPoint		ep;
				IPHostEntry		host;

				if (m_connstate == DHC_STATE_IDLE)
				{
					try
					{
						setConnState (DHC_STATE_CONNECTING);
						host = Dns.GetHostByName (m_hostname);
						addr = host.AddressList[0];
						m_offset = 0;
						ep = new IPEndPoint (addr, m_port);
						if (m_socket != null)
							m_socket.Close();
						m_socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream,
							ProtocolType.IP);
						onConnecting (m_hostname, m_port);
						Connect (ep, this);
					}
					catch (Exception e)
					{
						onConnectionFailure (m_hostname, m_port, e.ToString());
						m_socket.Close();
						result = -1;
					}
				}
				return (result);
			}

			private void RetryEventHandler(object sender, ElapsedEventArgs e)
			{
                if (m_TickMutex.WaitOne(0))
                {
                    try
                    {
                        if (m_connstate == DHC_STATE_IDLE)
                            openConnection();
                    }
                    finally
                    {
                        m_TickMutex.ReleaseMutex();
                    }
                }
			}

			public Exception cancelReconnectionTimer ()
			{
				Exception		result = null;

				try
				{
					if (m_retrytimer != null)
					{
						m_retrytimer.Stop();
						m_retrytimer = null;
					}
				}
				catch (Exception e)
				{
					result = e;
				}
				return (result);
			}

			public void startReconnectionTimer ()
			{
				if (m_retrytimer == null)
				{
					ElapsedEventHandler evhandler = new ElapsedEventHandler(RetryEventHandler);
					m_retrytimer = new System.Timers.Timer(m_reconnectdelay);
					m_retrytimer.AutoReset = true;
					m_retrytimer.Elapsed += evhandler;
				}
				m_retrytimer.Start();
			}

			public void setReconnectionDelay (int recon_ms)
			{
				if (recon_ms >= 0)
				{
					m_reconnectdelay = recon_ms;
					if (m_retrytimer != null)
						m_retrytimer.Interval = m_reconnectdelay;
				}
			}

			private int ConnectLoop ()
			{
				startReconnectionTimer();
				openConnection();
				return (0);
			}

			private void setConnState (int newstate)
			{
				int		oldstate = m_connstate;
				m_connstate = newstate;
				onStatusChange (oldstate, newstate);
			}

			public int getTimeout ()
			{
				return (m_timeout);
			}

			public int getHeartbeat ()
			{
				return (m_heartbeat);
			}

			public int getReconnectionDelay ()
			{
				return (m_reconnectdelay);
			}

			public String getHostname()
			{
				return (m_hostname);
			}
   	
			public int getPort()
			{
				return (m_port);
			}

			public bool isConnecting()
			{
				return (m_connstate == DHC_STATE_CONNECTING);
			}

			public bool isConnected()
			{
				return (m_connstate == DHC_STATE_CONNECTED);
			}

			private void setPointTimeStamp (DataHubPoint point)
			{
				point.setTimeStamp ();
			}

			private void setPointTimeStamp (DataHubPoint point, int seconds, int nanoseconds)
			{
				point.setTimeStamp (seconds, nanoseconds);
			}

			private void setPointTimeStamp (DataHubPoint point, DateTime datetime)
			{
				point.setTimeStamp (datetime);
			}

			public int setHeartbeatTimes (int heartbeat_ms, int timeout_ms)
			{
				m_heartbeat = heartbeat_ms;
				m_timeout = timeout_ms;

				if (m_alivetimer != null)
				{
					m_idletime = 0;
					m_alivetimer.Interval = m_heartbeat;
				}
				if (m_connstate == DHC_STATE_CONNECTED)
				{
					sendHeartbeatTimes ();
				}
				return (0);
			}

			public bool activeHeartbeatTimers()
			{
				return (m_alivetimer != null);
			}

			public void cancelHeartbeatTimers ()
			{
				if (m_alivetimer != null)
				{
					m_alivetimer.Stop();
					m_alivetimer = null;
				}
			}

			public void startHeartbeatTimers ()
			{
				if (m_heartbeat > 0)
				{
					if (m_alivetimer == null)
					{
						ElapsedEventHandler evhandler = new ElapsedEventHandler(HeartbeatEventHandler);
						m_alivetimer = new System.Timers.Timer(m_heartbeat);
						m_alivetimer.AutoReset = true;
						m_alivetimer.Elapsed += evhandler;
					}
					m_alivetimer.Start();
					m_idletime = 0;
				}
			}

			private void HeartbeatEventHandler(object sender, ElapsedEventArgs e)
			{
				if (m_connstate == DHC_STATE_CONNECTED)
				{
					m_idletime += m_heartbeat;
					if (m_idletime > m_timeout)
					{
						closeConnection("Inactivity timeout");
					}
					else if (!m_outputsent)
					{
						try
						{
							Send("(alive)\n");
						}
						catch (Exception ex)
						{
							closeConnection (ex.Message);
						}
					}
					m_outputsent = false;
				}
			}

			public String getCnxStateString (int cnxstate)
			{
				String	state = "None";
				switch (cnxstate)
				{
					case DHC_STATE_IDLE:
						state = "Idle";
						break;
					case DHC_STATE_CONNECTED:
						state = "Connected";
						break;
					case DHC_STATE_CONNECTING:
						state = "Connecting";
						break;
					case DHC_STATE_TERMINATING:
						state = "Terminating";
						break;
					case DHC_STATE_INITIALIZED:
						state = "Initialized";
						break;
					case DHC_STATE_CONN_RESOLVE_PORT_NAME:
						state = "Port name resolution";
						break;
					case DHC_STATE_CONN_ERR_PORT_FAIL:
						state = "Port name resolution failure";
						break;
					case DHC_STATE_CONN_RESOLVE_HOST_NAME:
						state = "Host name resolution";
						break;
					case DHC_STATE_CONN_ERR_HOST_FAIL:
						state = "Host name resolution failure";
						break;
					case DHC_STATE_CONN_ALLOCATE:
						state = "Allocating connection";
						break;
					case DHC_STATE_CONN_ERR_ALLOC_FAIL:
						state = "Allocating connection failure";
						break;
					case DHC_STATE_CONN_ERR_CONNECT_FAIL:
						state = "Connection failure";
						break;
					case DHC_STATE_CONN_ERR_SELECT_FAIL:
						state = "Select failure";
						break;
					case DHC_STATE_RETRY_DELAY:
						state = "Retry delay";
						break;
					default:
						state = "None";
						break;
				}
				return (state);
			}

			public int getCnxState ()
			{
				return (m_connstate);
			}

			/* String creation and parsing */

			/// <summary>
			/// Walk through a byte array of len bytes, breaking it up according to 
			/// parentheses and white space, respecting double quotes and backslash
			/// quoted characters.
			/// </summary>
			/// <param name="str">The raw input data</param>
			/// <param name="len">The number of characters in the input data</param>
			/// <param name="firstchar">An input/output array of one integer indicating the
			/// initial starting character within the string.  This is altered on return to
			/// indicate the new initial starting point for a subsequent call.</param>
			/// <returns>An array of Strings, containing the tokens in the input</returns>
			private String[] xxLispParse (byte[] str, int len, int[] firstchar)
			{
				String[]	result = new String[20];
				int			resind = 0;
				byte[]		word = new byte[len];
				int			strpos, wordpos=0, begin=-1, end=-1, done=0;
	   		
				for (strpos=firstchar[0]; strpos<len && done == 0; strpos++)
				{
                    switch ((char)str[strpos])
                    {
                        case ' ':
                        case '\t':
                        case '\n':
                        case '\r':
                            if (begin >= 0)
                                end = wordpos;
                            break;
                        case '(':
                        case ')':
                            if (begin >= 0)
                                done = 1;
                            break;
                        case '\"':
                            if (begin == -1)
                                begin = wordpos;
                            for (strpos++; str[strpos] != 0; strpos++)
                            {
                                if ((char)str[strpos] == '\\')
                                {
                                    switch ((char)str[++strpos])
                                    {
                                        case 'n':
                                            word[wordpos++] = (byte)'\n';
                                            break;
                                        case 'r':
                                            word[wordpos++] = (byte)'\r';
                                            break;
                                        case 't':
                                            word[wordpos++] = (byte)'\t';
                                            break;
                                        case 'f':
                                            word[wordpos++] = (byte)'\f';
                                            break;
                                        default:
                                            word[wordpos++] = str[strpos];
                                            break;
                                    }
                                }
                                else if ((char)str[strpos] == '\"')
                                {
                                    end = wordpos;
                                    break;
                                }
                                else
                                    word[wordpos++] = str[strpos];
                            }
                            break;
                        case '\\':
                            switch ((char)str[++strpos])
                            {
                                case 'n':
                                    word[wordpos++] = (byte)'\n';
                                    break;
                                case 'r':
                                    word[wordpos++] = (byte)'\r';
                                    break;
                                case 't':
                                    word[wordpos++] = (byte)'\t';
                                    break;
                                case 'f':
                                    word[wordpos++] = (byte)'\f';
                                    break;
                                default:
                                    word[wordpos++] = str[strpos];
                                    break;
                            }
                            break;
                        default:
                            if (begin == -1)
                                begin = wordpos;
                            word[wordpos++] = str[strpos];
                            break;
                    }
					if (end != -1)
					{
						result[resind++] = Encoding.UTF8.GetString (word, begin, end-begin);
						begin = end = -1;
						wordpos++;
					}
				}
				if (begin != -1)
				{
					end = wordpos;
					result[resind++] = Encoding.UTF8.GetString (word, begin, end-begin);
				}
				firstchar[0] = strpos >= len ? -1 : strpos;
				return (result);
			}

            private String[] LispParse(String original, out string remainder)
            {
                List<String> result = new List<String>();
                int strpos, done = 0, len = original.Length;
                String str = original;
                StringBuilder word = new StringBuilder(len);
                bool word_ended = false;

                for (strpos = 0; strpos < len && done == 0; strpos++)
                {
                    switch (str[strpos])
                    {
                        case ' ':
                        case '\t':
                        case '\n':
                        case '\r':
                            if (word.Length > 0)    // skip white space, or terminate a word
                                word_ended = true;
                            break;
                        case '(':
                        case ')':
                            if (word.Length > 0 || result.Count > 0)   // skip brackets if we have never encountered a word
                            {
                                if (word.Length > 0)
                                    word_ended = true;
                                done = 1;
                            }
                            break;
                        case '\"':
                            for (strpos++; str[strpos] != 0; strpos++)
                            {
                                if ((char)str[strpos] == '\\')
                                {
                                    switch ((char)str[++strpos])
                                    {
                                        case 'n':
                                            word.Append('\n');
                                            break;
                                        case 'r':
                                            word.Append('\r');
                                            break;
                                        case 't':
                                            word.Append('\t');
                                            break;
                                        case 'f':
                                            word.Append('\f');
                                            break;
                                        default:
                                            word.Append(str[strpos]);
                                            break;
                                    }
                                }
                                else if (str[strpos] == '\"')
                                {
                                    word_ended = true;
                                    break;
                                }
                                else
                                {
                                    word.Append(str[strpos]);
                                }
                            }
                            break;
                        case '\\':
                            switch (str[++strpos])
                            {
                                case 'n':
                                    word.Append('\n');
                                    break;
                                case 'r':
                                    word.Append('\r');
                                    break;
                                case 't':
                                    word.Append('\t');
                                    break;
                                case 'f':
                                    word.Append('\f');
                                    break;
                                default:
                                    word.Append(str[strpos]);
                                    break;
                            }
                            break;
                        default:
                            word.Append(str[strpos]);
                            break;
                    }
                    if (word_ended)
                    {
                        result.Add(word.ToString());
                        word.Length = 0;
                        word_ended = false;
                    }
                }
                if (word.Length > 0)
                {
                    result.Add(word.ToString());
                    word.Length = 0;
                }

                if (strpos < len)
                    remainder = original.Substring(strpos);
                else
                    remainder = null;
                return (result.ToArray());
            }


			public String escapedString (String str, bool quoted)
			{
				return (escapedString (str, quoted, (quoted ? true : false)));
			}

			public String escapedString (String str, bool quoted, bool special_only)
			{
				String		result;
				int			pos = 0;
				String		terminators;
				int			i, len = str.Length;
				char[]		buf = new char[len * 2 + 1 + (quoted ? 2 : 0)];

				if (special_only)
					terminators = "\t\n\f\r\\\"";
				else
					terminators = "() \t\n\f\r\\\"";
   		
				if (quoted)
					buf[pos++] = '\"';

				for (i=0; i<len; i++)
				{
					if (terminators.IndexOf(str[i]) != -1)
					{
						buf[pos++] = '\\';
						switch (str[i])
						{
							case '\t':
								buf[pos++] = 't';
								break;
							case '\n':
								buf[pos++] = 'n';
								break;
							case '\f':
								buf[pos++] = 'f';
								break;
							case '\r':
								buf[pos++] = 'r';
								break;
							default:
								buf[pos++] = str[i];
								break;
						}
					}
					else
						buf[pos++] = str[i];
				}

				if (quoted)
					buf[pos++] = '\"';
				result = new String(buf,0,pos);
				return (result);
			}

			/* Outgoing messages - Generic message support */

			public Exception writeCommand (String command)
			{
				if (command[command.Length-1] != '\n')
					command += "\n";
				return (Send (command));
			}

			/* Outgoing messages - specific DataHub commands */

			public Exception addPointValue (String pointname, double value)
			{
				return (mathPointValue ("add", pointname, value, 0, 0));
			}
 	
			public Exception addPointValue (DataHubPoint point, double value)
			{
				return (mathPointValue ("add", point.Name, value, 
					point.Seconds, point.Nanoseconds));
			}
 
			public Exception appendPointValue (String pointname, String value)
			{
				return (writeCommand ("(append " + 
					escapedString(pointname, false) + " " +
					escapedString(value, true) + " 0 0"));
			}

			public Exception appendPointValue (DataHubPoint point, String value)
			{
				return (writeCommand ("(append " + 
					escapedString(point.Name, false) + " " +
					escapedString(value, true) + " " +
					point.Seconds + " " + point.Nanoseconds + ")"));
			}
  	
			public Exception createPoint (String pointname)
			{
				return (writeCommand ("(create " + escapedString(pointname, false) + ")\n"));
			}

			public Exception createPoint (DataHubPoint point)
			{
				return createPoint (point.Name);
			}
   	
			public Exception dividePointValue (DataHubPoint point, double value)
			{
				return (mathPointValue ("div", point.Name, value, 
					point.Seconds, point.Nanoseconds));
			}
   	
			public Exception dividePointValue (String pointname, double value)
			{
				return (mathPointValue ("div", pointname, value, 0, 0));
			}
  	
			public Exception multiplyPointValue (DataHubPoint point, double value)
			{
				return (mathPointValue ("mult", point.Name, value, 
					point.Seconds, point.Nanoseconds));
			}
   	
			public Exception multiplyPointValue (String pointname, double value)
			{
				return (mathPointValue ("mult", pointname, value, 0, 0));
			}
   	
			private Exception mathPointValue (String operation, String pointname, 
				double value, long sec, long nsec)
			{
				return (writeCommand ("(" + operation + " " +
					escapedString(pointname, false) + " " +
					value + " " +
					sec + " " +
					nsec + ")"
					));
			}

			public Exception readPoint (DataHubPoint point, bool create, bool force)
			{
				return (readPoint (point.Name, create, force));
			}
  	
			public Exception readPoint (DataHubPoint point)
			{
				return (readPoint (point, false, true));
			}
  	
			public Exception readPoint (String pointname, bool create, bool force)
			{
				String		operation = (create ? "cread" : "read");
				return (writeCommand ("(" + operation + " " + 
					escapedString (pointname, false) + ")\n"));
			}
  	
			public Exception readPoint (String pointname)
			{
				return (readPoint (pointname, false, true));
			}
  	
			public const int		DH_REG_ALL		= 0x01;
			public const int		DH_REG_FUTURE	= 0x02;
			public const int		DH_REG_QUALIFY	= 0x04;
			public const int		DH_REG_ONCEONLY	= 0x08;
			public const int		DH_REG_MODEL	= 0x10;

			public Exception registerDomain (String domainname, int flags)
			{
				return (writeCommand ("(report_domain " + 
					escapedString(domainname, true) + " " + flags + ")\n"));
			}

			public Exception registerPoint (String pointname, bool create)
			{
				String		cmd = (create ? "creport" : "report");
				return (writeCommand ("(" + cmd + " " +
					escapedString(pointname, false) + ")\n"));
			}

			public Exception registerPoint (DataHubPoint point, bool create)
			{
				return registerPoint (point.Name, create);
			}

			private void sendHeartbeatTimes ()
			{
				writeCommand ("(heartbeat " + m_heartbeat + ") (timeout " + m_timeout + ")");
			}

            public string GetMD5Hash(string input)
            {
                System.Security.Cryptography.MD5CryptoServiceProvider x = new System.Security.Cryptography.MD5CryptoServiceProvider();
                byte[] bs = System.Text.Encoding.UTF8.GetBytes(input);
                bs = x.ComputeHash(bs);
                System.Text.StringBuilder s = new System.Text.StringBuilder();
                foreach (byte b in bs)
                {
                    s.Append(b.ToString("x2").ToLower());
                }
                string password = s.ToString();
                return password;
            }

			public Exception sendLogin ()
			{
				if (m_username != null && m_password != null)
				{
                    String hashed = GetMD5Hash(m_password);
					return (writeCommand("(auth" +
						escapedString(m_username, true) + " " +
						escapedString(hashed, true) + ")\n"));
				}
				else
					return (null);
			}

			private void sendWantSuccess ()
			{
				writeCommand ("(acksuccess " + (m_wantsuccess ? 1 : 0) + ")");
			}

			public Exception setDefaultDomain(String domain)
			{
                if (!String.IsNullOrEmpty(domain))
                {
                    m_domain = domain;
                    return (writeCommand("(domain " + escapedString(domain, true) + ")"));
                }
                else
                {
                    return new Exception("No default domain set");
                }
			}

			public Exception setPointLock (DataHubPoint point, bool locked)
			{
				return (setPointLock (point.Name, point.Security, locked));
			}
  	
			public Exception setPointLock (String pointname, int security, bool locked)
			{
				return (writeCommand ("(lock " + 
					escapedString(pointname, false) + " " +
					security + " " +
					(locked ? 1 : 0) + ")\n"));
			}

			public Exception setPointSecurity (String pointname, int my_security, int new_security)
			{
				return (writeCommand ("(secure " + 
					escapedString(pointname, false) + " " +
					my_security + " " +
					new_security + ")\n"));
			}
  	
			public Exception setPointSecurity (DataHubPoint point, int new_security)
			{
				return (writeCommand ("(secure " + 
					escapedString(point.Name, false) + " " +
					point.Security + " " +
					new_security + ")\n"));
			}

			public Exception unregisterPoint (String pointname)
			{
				return (writeCommand ("(unreport " + 
					escapedString(pointname, false) + ")\n"));
			}
  	
			public Exception unregisterPoint (DataHubPoint point)
			{
				return (unregisterPoint (point.Name));
			}

            private Exception writePoint(String pointname, int type, String strval,
                int seconds, int nanoseconds, int quality, bool create, bool force)
            {
                String command = (create ? "cwrite" : "write");
                return (writeCommand("(" + command + " " +
                    escapedString(pointname, false) + " " +
                    type.ToString(CultureInfo.InvariantCulture.NumberFormat) + " " +
                    (type == DataHubPoint.PT_TYPE_STRING ? escapedString(strval, true) : strval) +
                    " 100 0 0 " + seconds + " " + nanoseconds + " 0 " +
                    quality +
                    ")"));
            }

			public Exception writePoint (String pointname, int type, String strval,
				bool create, bool force)
			{
                return (writePoint(pointname, type, strval, 0, 0, DataHubPoint.PT_QUALITY_GOOD, create, force));
			}

			public Exception writePoint (DataHubPoint point, bool create, bool force)
			{
				return (writePoint (point.Name, point.Type, point.StrVal, point.Seconds,
                    point.Nanoseconds, DataHubPoint.PT_QUALITY_GOOD, create, force));
			}

			public Exception writePoint (String pointname, int value, bool create, bool force)
			{
				return (writePoint (pointname, DataHubPoint.PT_TYPE_INT32, value.ToString(CultureInfo.InvariantCulture.NumberFormat), create, force));
			}

			public Exception writePoint (String pointname, double value, bool create, bool force)
			{
				return (writePoint (pointname, DataHubPoint.PT_TYPE_REAL, value.ToString(CultureInfo.InvariantCulture.NumberFormat), create, force));
			}

			public Exception writePoint (String pointname, String value, bool create, bool force)
			{
				return (writePoint (pointname, DataHubPoint.PT_TYPE_STRING, value, create, force));
			}

			public Exception writePoint (String pointname, String value)
			{
				return writePoint (pointname, value, true, false);
			}

			public Exception writePoint (String pointname, double value)
			{
				return writePoint (pointname, value, true, false);
			}

			public Exception writePoint (String pointname, int value)
			{
				return writePoint (pointname, value, true, false);
			}

            public Exception writePoint (String pointname, int type, String value, int quality, DateTime timestamp, bool create, bool force)
            {
                int secs, nsecs;
                DataHubPoint.WindowsTimeToUnixTime(timestamp.ToOADate(), out secs, out nsecs);
                return writePoint(pointname, type, value, secs, nsecs, quality, create, force);
            }

            public Exception writePoint(String pointname, String value, int quality, DateTime timestamp, bool create, bool force)
            {
                return writePoint(pointname, DataHubPoint.PT_TYPE_STRING, value, quality, timestamp, create, force);
            }

            public Exception writePoint(String pointname, double value, int quality, DateTime timestamp, bool create, bool force)
            {
                return writePoint(pointname, DataHubPoint.PT_TYPE_REAL, value.ToString(), quality, timestamp, create, force);
            }

            public Exception writePoint(String pointname, int value, int quality, DateTime timestamp, bool create, bool force)
            {
                return writePoint(pointname, DataHubPoint.PT_TYPE_INT32, value.ToString(), quality, timestamp, create, force);
            }

			// Callbacks for event handling
			
			public void SetISynchronizeInvoke(ISynchronizeInvoke iface)
			{
				m_ISynchronizeInvoke = iface;
			}

			delegate void onPointChangeDelegate(DataHubPoint point);
			delegate void onPointEchoDelegate(DataHubPoint point);
			delegate void onAsyncMessageDelegate(String[] arguments);
			delegate void onAliveDelegate();
			delegate void onSuccessDelegate(String cmd, String parms);
			delegate void onErrorDelegate(int status, String message, String cmd, String parms);
			delegate void onConnectionSuccessDelegate(String host, int port);
			delegate void onConnectionFailureDelegate(String host, int port, String reason);
			delegate void onConnectingDelegate(String host, int port);
			delegate void onStatusChangeDelegate(int oldstate, int newstate);
            delegate void ProcessMessageDelegate(String[] args);

			public virtual void onPointChange (DataHubPoint point)
			{
				if (m_ISynchronizeInvoke != null && m_ISynchronizeInvoke.InvokeRequired)
				{
					onPointChangeDelegate d = new onPointChangeDelegate(onPointChange);
					m_ISynchronizeInvoke.BeginInvoke(d, new object[] { point });
				}
				else
				{
					if (m_target != null)
						m_target.onPointChange(point);
				}
			}

			public virtual void onPointEcho (DataHubPoint point)
			{
				if (m_ISynchronizeInvoke != null && m_ISynchronizeInvoke.InvokeRequired)
				{
					onPointChangeDelegate d = new onPointChangeDelegate(onPointEcho);
					m_ISynchronizeInvoke.BeginInvoke(d, new object[] { point });
				}
				else
				{
					if (m_target != null)
						m_target.onPointEcho(point);
				}
			}

			public virtual void onAsyncMessage (String[] arguments)
			{
				if (m_ISynchronizeInvoke != null && m_ISynchronizeInvoke.InvokeRequired)
				{
					onAsyncMessageDelegate d = new onAsyncMessageDelegate(onAsyncMessage);
					m_ISynchronizeInvoke.BeginInvoke(d, new object[] { arguments });
				}
				else
				{
					if (m_target != null)
						m_target.onAsyncMessage(arguments);
				}
			}

			public virtual void onAlive ()
			{
				if (m_ISynchronizeInvoke != null && m_ISynchronizeInvoke.InvokeRequired)
				{
					onAliveDelegate d = new onAliveDelegate(onAlive);
					m_ISynchronizeInvoke.BeginInvoke(d, new object[] { });
				}
				else
				{
					if (m_target != null)
						m_target.onAlive();
				}
			}

			public virtual void onSuccess (String cmd, String parms)
			{
				if (m_ISynchronizeInvoke != null && m_ISynchronizeInvoke.InvokeRequired)
				{
					onSuccessDelegate d = new onSuccessDelegate(onSuccess);
					m_ISynchronizeInvoke.BeginInvoke(d, new object[] { cmd, parms });
				}
				else
				{
					if (m_target != null)
						m_target.onSuccess(cmd, parms);
				}
			}

			public virtual void onError (int status, String message, String cmd, String parms)
			{
				if (m_ISynchronizeInvoke != null && m_ISynchronizeInvoke.InvokeRequired)
				{
					onErrorDelegate d = new onErrorDelegate(onError);
					m_ISynchronizeInvoke.BeginInvoke(d, new object[] { status, message, cmd, parms });
				}
				else
				{
					if (m_target != null) 
						m_target.onError(status, message, cmd, parms);
				}
			}

			public virtual void onConnectionSuccess (String host, int port)
			{
				if (m_ISynchronizeInvoke != null && m_ISynchronizeInvoke.InvokeRequired)
				{
					onConnectionSuccessDelegate d = new onConnectionSuccessDelegate(onConnectionSuccess);
					m_ISynchronizeInvoke.BeginInvoke(d, new object[] { host, port });
				}
				else
				{
					if (m_target != null)
						m_target.onConnectionSuccess(host, port);
				}
			}

			public virtual void onConnectionFailure (String host, int port, String reason)
			{
				if (m_ISynchronizeInvoke != null && m_ISynchronizeInvoke.InvokeRequired)
				{
					//setConnState (DHC_STATE_CONN_ERR_CONNECT_FAIL);
					onConnectionFailureDelegate d = new onConnectionFailureDelegate(onConnectionFailure);
					m_ISynchronizeInvoke.BeginInvoke(d, new object[] { host, port, reason });
				}
				else
				{
					if (m_target != null) 
						m_target.onConnectionFailure(host, port, reason);
                    // This is running in the mainline.  It should not be setting the connection status.
					//setConnState (DHC_STATE_IDLE);
				}
			}

			public virtual void onConnecting (String host, int port)
			{
				if (m_ISynchronizeInvoke != null && m_ISynchronizeInvoke.InvokeRequired)
				{
					onConnectingDelegate d = new onConnectingDelegate(onConnecting);
					m_ISynchronizeInvoke.BeginInvoke(d, new object[] { host, port });
				}
				else
				{
					if (m_target != null) 
						m_target.onConnecting(host, port);
				}
			}

			public virtual void onStatusChange (int oldstate, int newstate)
			{
				if (m_ISynchronizeInvoke != null && m_ISynchronizeInvoke.InvokeRequired)
				{
					onStatusChangeDelegate d = new onStatusChangeDelegate(onStatusChange);
					m_ISynchronizeInvoke.BeginInvoke(d, new object[] { oldstate, newstate });
				}
				else
				{
					if (m_target != null)
						m_target.onStatusChange(oldstate, newstate);
				}
			}

			/* Asynchronous events across threads: ISynchronousInvoke implementation */

			// Ideally we would implement ISynchronousInvoke ourselves, but that appears
			// to be out of the question.  There is not one implementation available on
			// the web that fits this model, and Microsoft offers no examples.
//			public bool InvokeRequired
//			{
//				get
//				{
//					return Thread.CurrentThread.ManagedThreadId != 
//						delegateThread.ManagedThreadId;
//				}
//			}
//
//			public IAsyncResult BeginInvoke (Delegate d, object[] args)
//			{
//
//			}
//
//			public object EndInvoke(IAsyncResult iar)
//			{
//			}
//
//			public void Invoke(Delegate d, objec[] args)
//			{
//				IAsyncResult	iar = BeginInvoke(d, args);
//				object			result = EndInvoke(iar);
//				return result;
//			}
		}
	}
}
