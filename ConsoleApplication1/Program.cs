using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace DualSocketServer
{
    class Program
    {
        //
        // State object for reading client data asynchronously
        public class StateObject
        {
            // Client  socket.
            public Socket workSocket = null;
            // Size of receive buffer.
            public const int BufferSize = 1024;
            // Receive buffer.
            public byte[] buffer = new byte[BufferSize];
            // Received data string.
            public StringBuilder sb = new StringBuilder();
        }

        public class Host1
        {
            public static IPHostEntry ipHostInfo = Dns.Resolve(Dns.GetHostName());
            public static IPAddress ipAddress = ipHostInfo.AddressList[0];
            public IPEndPoint localEndPoint = new IPEndPoint(ipAddress, 5000);
            // Create a TCP/IP socket.
            public Socket listener = new Socket(AddressFamily.InterNetwork,
                SocketType.Stream, ProtocolType.Tcp);
        }
        public class Host2
        {
            public static IPHostEntry ipHostInfo = Dns.Resolve(Dns.GetHostName());
            public static IPAddress ipAddress = ipHostInfo.AddressList[0];
            public IPEndPoint localEndPoint = new IPEndPoint(ipAddress, 5001);
            // Create a TCP/IP socket.
            public Socket listener = new Socket(AddressFamily.InterNetwork,
                SocketType.Stream, ProtocolType.Tcp);
        }

        public class AsynchronousSocketListener
        {
            // Thread signal.
            public static ManualResetEvent allDone = new ManualResetEvent(false);

            public static Host1 host1 = new Host1();
            public static Host2 host2 = new Host2();

            public static void StartListening()
            {
                // Data buffer for incoming data.
                byte[] bytes = new Byte[1024];
                
                byte[] bytes2 = new Byte[1024];
                
                // Bind the socket to the local endpoint and listen for incoming connections.
                try
                {
                    host1.listener.Bind(host1.localEndPoint);
                    host1.listener.Listen(100);

                    host2.listener.Bind(host2.localEndPoint);
                    host2.listener.Listen(100);

                    while (true)
                    {
                        // Set the event to nonsignaled state.
                        allDone.Reset();

                        // Start an asynchronous socket to listen for connections.
                        Console.WriteLine("Waiting for a connection... "+ host1.localEndPoint);
                        host1.listener.BeginAccept(
                            new AsyncCallback(AcceptCallback),
                            host1.listener);

                        Console.WriteLine("Waiting for a connection...2 " + host2.localEndPoint);
                        host2.listener.BeginAccept(
                            new AsyncCallback(AcceptCallback),
                            host2.listener);

                        // Wait until a connection is made before continuing.
                        allDone.WaitOne();
                    }

                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }

                Console.WriteLine("\nPress ENTER to continue...");
                Console.Read();

            }

            public static void AcceptCallback(IAsyncResult ar)
            {
                // Signal the main thread to continue.
                allDone.Set();
                IAsyncResult arAux= ar;

                // Get the socket that handles the client request.
                Socket listen = (Socket)arAux.AsyncState;
               
                // Create the state object.
                StateObject st = new StateObject();
                st.workSocket = listen;

                Console.WriteLine("{0}  {1}   {2}", st.workSocket.LocalEndPoint, 
                    host1.listener.LocalEndPoint, 
                    st.workSocket.LocalEndPoint.Equals(host1.listener.LocalEndPoint));

                // Get the socket that handles the client request.
                Socket listener;
                Socket handler;

                if (st.workSocket.LocalEndPoint.Equals(host1.listener.LocalEndPoint))//Entra si es por el 5000
                {
                    listener = host1.listener;//(Socket)ar.AsyncState;
                    handler = host1.listener.EndAccept(ar);
                } 
                else                                                            //Entra si es por 5001
                {
                    listener = host2.listener;//(Socket)ar.AsyncState;
                    handler = host2.listener.EndAccept(ar);
                }
                // Create the state object.
                StateObject state = new StateObject();
                state.workSocket = handler;

                handler.BeginReceive(state.buffer, 0, StateObject.BufferSize, 0,
                    new AsyncCallback(ReadCallback), state);
            }

            public static void ReadCallback(IAsyncResult ar)
            {
                String content = String.Empty;

                // Retrieve the state object and the handler socket
                // from the asynchronous state object.
                StateObject state = (StateObject)ar.AsyncState;
                Socket handler = state.workSocket;
                
                // Read data from the client socket.
                int bytesRead = handler.EndReceive(ar);

                if (bytesRead > 0)
                {
                    // There  might be more data, so store the data received so far.
                    state.sb.Append(Encoding.ASCII.GetString(
                        state.buffer, 0, bytesRead));

                    // Check for end-of-file tag. If it is not there, read 
                    // more data.
                    content = state.sb.ToString();
                    Console.WriteLine("\nLeido {0} bytes from socket. \n Data : {1}\n",
                            content.Length, content);
                    if (content.IndexOf("\r") > -1)
                    {
                        // All the data has been read from the 
                        // client. Display it on the console.
                        Console.WriteLine("Read {0} bytes from socket. \n Data : {1}",
                            content.Length, content);
                        // Echo the data back to the client.
                        Send(handler, content);

                        state.sb.Clear();//limpia para esperar nuevo dato.
                       
                        handler.BeginReceive(state.buffer, 0, StateObject.BufferSize, 0,
                       new AsyncCallback(ReadCallback), state);

                    }
                    else
                    {
                        // Not all data received. Get more.
                        handler.BeginReceive(state.buffer, 0, StateObject.BufferSize, 0,
                        new AsyncCallback(ReadCallback), state);
                    }
                }
                
            }

            /// <summary>
            /// Envia data al port 5000
            /// </summary>
            /// <param name="handler"></param>
            /// <param name="data"></param>
            private static void Send(Socket handler, String data)
            {
                // Convert the string data to byte data using ASCII encoding.
                byte[] byteData = Encoding.ASCII.GetBytes(data);

                // Begin sending the data to the remote device.
                handler.BeginSend(byteData, 0, byteData.Length, 0,
                    new AsyncCallback(SendCallback), handler);
            }

            private static void SendCallback(IAsyncResult ar)
            {
                try
                {
                    // Retrieve the socket from the state object.
                    Socket handler = (Socket)ar.AsyncState;

                    // Complete sending the data to the remote device.
                    int bytesSent = handler.EndSend(ar);
                    Console.WriteLine("Sent {0} bytes to client.", bytesSent);

                    
                    //handler.Shutdown(SocketShutdown.Both);
                    //handler.Close();

                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
            }

            public static int Main(String[] args)
            {
                StartListening();
                
                return 0;
            }
        }

    }
}




