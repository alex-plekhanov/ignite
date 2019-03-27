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

package org.apache.ignite.internal.processors.platform.client;

import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.transactions.Transaction;

/**
 * Thin client request handler worker to maintain single threaded transactional execution.
 */
class ClientRequestHandlerWorker extends GridWorker {
    /** Requests queue.*/
    private final LinkedBlockingQueue<ProcessRequestFuture> queue = new LinkedBlockingQueue<>();

    /** Context.*/
    private final ClientConnectionContext ctx;

    /** Worker is started. */
    private volatile boolean started;

    /**
     * Constructor.
     * @param ctx Client connection context.
     */
    ClientRequestHandlerWorker(ClientConnectionContext ctx) {
        super(ctx.kernalContext().igniteInstanceName(), "client-request-handler-worker",
            ctx.kernalContext().log(ClientRequestHandlerWorker.class));

        this.ctx = ctx;
    }

    /**
     * Start this worker.
     */
    private synchronized void start() {
        if (!started) {
            new IgniteThread(this).start();

            started = true;
        }
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        try {
            while (!isCancelled())
                queue.take().process();
        }
        finally {
            try {
                T2<Transaction, Integer> txCtx = ctx.txContext();

                if (txCtx != null) {
                    txCtx.get1().rollback();
                    txCtx.get1().close();
                }
            }
            catch (Exception e) {
                // No-op.
            }
            finally {
                ctx.txContext(null);
            }

            // Drain the queue on stop.
            for (ProcessRequestFuture fut = queue.poll(); fut != null; fut = queue.poll())
                fut.onDone(new ClientResponse(fut.req.requestId(), "Request worker stopped"));
        }
    }

    /**
     * Initiate request processing.
     * @param req Request.
     * @return Future to track request processing.
     */
    IgniteInternalFuture<ClientListenerResponse> process(ClientRequest req) {
        if (!started)
            start();

        ProcessRequestFuture fut = new ProcessRequestFuture(req);

        queue.add(fut);

        return fut;
    }

    /**
     *
     */
    private class ProcessRequestFuture extends GridFutureAdapter<ClientListenerResponse> {
        /** Request. */
        private final ClientRequest req;

        /**
         * @param req Request.
         */
        private ProcessRequestFuture(ClientRequest req) {
            this.req = req;
        }

        /**
         * Process request.
         */
        private void process() {
            try {
                onDone(req.process(ctx));
            }
            catch (Exception e) {
                onDone(e);
            }
        }
    }
}
