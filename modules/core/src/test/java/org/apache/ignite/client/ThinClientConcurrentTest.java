/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

@WithSystemProperty(key = IgniteSystemProperties.IGNITE_NO_SELECTOR_OPTS, value = "false")
public class ThinClientConcurrentTest extends GridCommonAbstractTest {
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }


    @Test
    public void testConcurrentLoad() throws Exception {
        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800")
            //.setTcpNoDelay(true)
            //.setSendBufferSize(0)
        )) {
            ClientCache<Integer, String> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

            AtomicInteger queueSize = new AtomicInteger();
            AtomicInteger threadCnt = new AtomicInteger();

            GridTestUtils.runMultiThreaded(
                () -> {
                    int thread = threadCnt.incrementAndGet();

                    for (int i = 0; i < 1000; i++) {
                        log.info("Thread " + thread + " val " + i);

/*
                        if (queueSize.incrementAndGet() > 4)
                            log.info("Blocked");
*/

                        cache.put(i, "Thread " + thread + " val " + i);

//                        queueSize.decrementAndGet();
                    }
                }, 5, "run-async");
        }
    }

    @Test
    public void testSocket() throws Exception {
        EchoServer server = new EchoServer(12345);
        Thread serverThread = new Thread(server);
        serverThread.start();

        Client client = new Client("127.0.0.1", 12345);
        Thread clientThread = new Thread(() -> {
            try {
                while (!client.stopped())
                    client.receive();
            }
            catch (IOException e) {
                log.error("Error in receiver thread", e);
            }
        });
        clientThread.start();

        GridTestUtils.runMultiThreaded(
            () -> {
                try {
                    Random rnd = new Random();
                    while (!client.stopped())
                        client.send((byte)rnd.nextInt());
                }
                catch (IOException e) {
                    log.error("Error in sender thread", e);
                }

            }, 5, "run-async");
    }

    public class EchoServer implements Runnable {
        private final ServerSocket sock;
        private final AtomicBoolean stopped = new AtomicBoolean();

        EchoServer(int port) throws IOException {
            sock = new ServerSocket(port);
        }

        @Override public void run() {
            Socket acceptedSock = null;
            OutputStream out = null;
            InputStream in = null;

            try {
                acceptedSock = sock.accept();
                out = acceptedSock.getOutputStream();
                in = acceptedSock.getInputStream();

                while (!stopped.get()) {
                    byte[] buf = new byte[1];

                    int readCnt = in.read(buf, 0, 1);

                    if (readCnt > 0)
                        out.write(buf);

                    System.out.println(buf[0]);
                }
            }
            catch (IOException ignore) {
            }
            finally {
                try {
                    if (in != null)
                        in.close();

                    if (out != null)
                        out.close();

                    if (acceptedSock != null)
                        acceptedSock.close();

                    sock.close();
                }
                catch (IOException ignore) {
                }
            }
        }

        public void stop() {
            stopped.set(true);
        }
    }

    public class Client implements Closeable {
        private final Socket sock;
        private final OutputStream out;
        private final InputStream in;
        private final Object recvMux = new Object();
        private final Object sendMux = new Object();
        private final AtomicBoolean stopped = new AtomicBoolean();
        private volatile long sentBytes;
        private volatile long recvBytes;

        Client(String ip, int port) throws IOException {
            sock = new Socket(ip, port);
            out = sock.getOutputStream();
            in = sock.getInputStream();
        }

        public void send(byte b) throws IOException {
            synchronized (sendMux){
                byte[] buf = new byte[1];
                buf[0] = b;
                out.write(buf);
                out.flush();
                sentBytes++;
            }
        }

        public byte receive() throws IOException {
            synchronized (recvMux){
                byte[] buf = new byte[1];
                in.read(buf, 0, 1);
                recvBytes++;
                return buf[0];
            }
        }

        @Override public void close() {
            stopped.set(true);
            try {
                in.close();
                out.close();
                sock.close();
            }
            catch (IOException ignore) {
            }
        }

        public boolean stopped() {
            return stopped.get();
        }

        public long getSentBytes() {
            return sentBytes;
        }

        public long getRecvBytes() {
            return recvBytes;
        }
    }
}
