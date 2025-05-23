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

package org.apache.ignite.internal.util.nio.ssl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.util.nio.GridNioException;
import org.apache.ignite.internal.util.nio.GridNioFilterAdapter;
import org.apache.ignite.internal.util.nio.GridNioFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioFutureImpl;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.SSL_META;

/**
 * Implementation of SSL filter using {@link SSLEngine}
 */
public class GridNioSslFilter extends GridNioFilterAdapter {
    /** SSL handshake future metadata key. */
    public static final int HANDSHAKE_FUT_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** The name of the metric that provides histogram of SSL handshake duration. */
    public static final String SSL_HANDSHAKE_DURATION_HISTOGRAM_METRIC_NAME = "SslHandshakeDurationHistogram";

    /** The name of the metric that provides sessions count that were rejected due to SSL errors. */
    public static final String SSL_REJECTED_SESSIONS_CNT_METRIC_NAME = "RejectedSslSessionsCount";

    /** Logger to use. */
    private IgniteLogger log;

    /** Set to true if engine should request client authentication. */
    private boolean wantClientAuth;

    /** Set to true if engine should require client authentication. */
    private boolean needClientAuth;

    /** Array of enabled cipher suites, optional. */
    private String[] enabledCipherSuites;

    /** Array of enabled protocols. */
    private String[] enabledProtos;

    /** SSL context to use. */
    private SSLContext sslCtx;

    /** Order. */
    private ByteOrder order;

    /** Allocate direct buffer or heap buffer. */
    private boolean directBuf;

    /** Whether direct mode is used. */
    private boolean directMode;

    /** Exception during onSessionOpened */
    @Nullable private Exception onSessionOpenedException;

    /** Metric that indicates sessions count that were rejected due to SSL errors. */
    @Nullable private final IntMetricImpl rejectedSesCnt;

    /** Histogram that provides distribution of SSL handshake duration. */
    @Nullable private final HistogramMetricImpl handshakeDuration;

    /**
     * Creates SSL filter.
     *
     * @param sslCtx SSL context.
     * @param directBuf Direct buffer flag.
     * @param order Byte order.
     * @param log Logger to use.
     * @param mreg Optional metric registry.
     */
    public GridNioSslFilter(
        SSLContext sslCtx,
        boolean directBuf,
        ByteOrder order,
        IgniteLogger log,
        @Nullable MetricRegistryImpl mreg
    ) {
        super("SSL filter");

        this.log = log;
        this.sslCtx = sslCtx;
        this.directBuf = directBuf;
        this.order = order;

        handshakeDuration = mreg == null ? null : mreg.histogram(
            SSL_HANDSHAKE_DURATION_HISTOGRAM_METRIC_NAME,
            new long[] {250, 500, 1000},
            "SSL handshake duration in milliseconds."
        );

        rejectedSesCnt = mreg == null ? null : mreg.intMetric(
            SSL_REJECTED_SESSIONS_CNT_METRIC_NAME,
            "TCP sessions count that were rejected due to SSL errors."
        );
    }

    /**
     * @param directMode Flag indicating whether direct mode is used.
     */
    public void directMode(boolean directMode) {
        this.directMode = directMode;
    }

    /**
     * @return Flag indicating whether direct mode is used.
     */
    public boolean directMode() {
        return directMode;
    }

    /**
     * Sets flag indicating whether client authentication will be requested during handshake.
     *
     * @param wantClientAuth {@code True} if client authentication should be requested.
     */
    public void wantClientAuth(boolean wantClientAuth) {
        this.wantClientAuth = wantClientAuth;
    }

    /**
     * Sets flag indicating whether client authentication will be required.
     *
     * @param needClientAuth {@code True} if client authentication is required.
     */
    public void needClientAuth(boolean needClientAuth) {
        this.needClientAuth = needClientAuth;
    }

    /**
     * Sets a set of cipher suites that will be enabled for this filter.
     *
     * @param enabledCipherSuites Enabled cipher suites.
     */
    public void enabledCipherSuites(String... enabledCipherSuites) {
        this.enabledCipherSuites = enabledCipherSuites;
    }

    /**
     * Sets enabled secure protocols for this filter.
     *
     * @param enabledProtos Enabled protocols.
     */
    public void enabledProtocols(String... enabledProtos) {
        this.enabledProtos = enabledProtos;
    }

    /** {@inheritDoc} */
    @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Remote client connected, creating SSL handler and performing initial handshake: " + ses);

        SSLEngine engine;

        boolean handshake;

        GridSslMeta sslMeta = ses.meta(SSL_META.ordinal());

        if (sslMeta == null) {
            try {
                engine = sslCtx.createSSLEngine();
            }
            catch (IllegalArgumentException e) {
                IgniteCheckedException ex = new IgniteCheckedException("Failed connect to cluster. Check SSL configuration.", e);
                onSessionOpenedException = ex;
                throw ex;
            }

            boolean clientMode = !ses.accepted();

            engine.setUseClientMode(clientMode);

            if (!clientMode) {
                engine.setWantClientAuth(wantClientAuth);

                engine.setNeedClientAuth(needClientAuth);
            }

            if (enabledCipherSuites != null)
                engine.setEnabledCipherSuites(enabledCipherSuites);

            if (enabledProtos != null)
                engine.setEnabledProtocols(enabledProtos);

            sslMeta = new GridSslMeta();

            sslMeta.sslEngine(engine);

            ses.addMeta(SSL_META.ordinal(), sslMeta);

            handshake = true;
        }
        else {
            engine = sslMeta.sslEngine();

            assert engine != null;

            handshake = false;
        }

        try {
            GridNioSslHandler hnd = new GridNioSslHandler(
                this,
                ses,
                engine,
                directBuf,
                order,
                log,
                handshake,
                sslMeta);

            sslMeta.handler(hnd);

            if (handshakeDuration != null) {
                GridNioFutureImpl<?> fut = ses.meta(HANDSHAKE_FUT_META_KEY);

                if (fut == null) {
                    fut = new GridNioFutureImpl<>(null);

                    ses.addMeta(HANDSHAKE_FUT_META_KEY, fut);
                }

                long startTime = System.nanoTime();

                fut.listen(() -> handshakeDuration.value(U.nanosToMillis(System.nanoTime() - startTime)));
            }

            hnd.handshake();

            processApplicationBuffer(ses, hnd.getApplicationBuffer());
        }
        catch (SSLException e) {
            onSessionOpenedException = e;
            U.error(log, "Failed to start SSL handshake (will close inbound connection): " + ses, e);

            ses.close();
        }
    }

    /** {@inheritDoc} */
    @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
        try {
            GridNioFutureImpl<?> fut = ses.removeMeta(HANDSHAKE_FUT_META_KEY);

            if (fut != null) {
                if (rejectedSesCnt != null)
                    rejectedSesCnt.increment();

                fut.onDone(new IgniteCheckedException("SSL handshake failed (connection closed).", onSessionOpenedException));
            }

            if (ses.meta(SSL_META.ordinal()) == null)
                return;

            GridNioSslHandler hnd = sslHandler(ses);

            hnd.shutdown();
        }
        finally {
            proceedSessionClosed(ses);
        }
    }

    /** {@inheritDoc} */
    @Override public void onExceptionCaught(GridNioSession ses, IgniteCheckedException ex)
        throws IgniteCheckedException {
        proceedExceptionCaught(ses, ex);
    }

    /**
     * @param ses Session.
     * @return SSL handshake flag.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    public boolean lock(GridNioSession ses) {
        GridNioSslHandler hnd = sslHandler(ses);

        hnd.lock();

        return hnd.isHandshakeFinished();
    }

    /**
     * @param ses NIO session.
     */
    public void unlock(GridNioSession ses) {
        sslHandler(ses).unlock();
    }

    /**
     * @param ses Session.
     * @param input Data to encrypt.
     * @return Output buffer with encrypted data.
     * @throws SSLException If failed to encrypt.
     */
    public ByteBuffer encrypt(GridNioSession ses, ByteBuffer input) throws SSLException {
        GridNioSslHandler hnd = sslHandler(ses);

        hnd.lock();

        try {
            assert hnd.isHandshakeFinished();

            return hnd.encrypt(input);
        }
        finally {
            hnd.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onSessionWrite(
        GridNioSession ses,
        Object msg,
        boolean fut,
        IgniteInClosure<IgniteException> ackC
    ) throws IgniteCheckedException {
        if (directMode)
            return proceedSessionWrite(ses, msg, fut, ackC);

        ByteBuffer input = checkMessage(ses, msg);

        if (!input.hasRemaining())
            return new GridNioFinishedFuture<Object>(null);

        GridNioSslHandler hnd = sslHandler(ses);

        hnd.lock();

        try {
            if (hnd.isOutboundDone())
                return new GridNioFinishedFuture<Object>(new IOException("Failed to send data (secure session was " +
                    "already closed): " + ses));

            if (hnd.isHandshakeFinished()) {
                hnd.encrypt(input);

                return hnd.writeNetBuffer(ackC);
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Write request received during handshake, scheduling deferred write: " + ses);

                return hnd.deferredWrite(input, ackC);
            }
        }
        catch (SSLException e) {
            throw new GridNioException("Failed to encode SSL data: " + ses, e);
        }
        finally {
            hnd.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        ByteBuffer input = checkMessage(ses, msg);

        GridNioSslHandler hnd = sslHandler(ses);

        hnd.lock();

        try {
            hnd.messageReceived(input);

            // Handshake may become finished on incoming message, flush writes, if any.
            if (hnd.isHandshakeFinished())
                hnd.flushDeferredWrites();

            processApplicationBuffer(ses, hnd.getApplicationBuffer());

            if (hnd.isInboundDone() && !hnd.isOutboundDone()) {
                if (log.isDebugEnabled())
                    log.debug("Remote peer closed secure session (will close connection): " + ses);

                shutdownSession(ses, hnd);
            }
        }
        catch (SSLException e) {
            throw new GridNioException("Failed to decode SSL data: " + ses, e);
        }
        finally {
            hnd.unlock();
        }
    }

    /** */
    private void processApplicationBuffer(GridNioSession ses, ByteBuffer appBuffer) throws IgniteCheckedException {
        appBuffer.flip();

        if (appBuffer.hasRemaining())
            proceedMessageReceived(ses, appBuffer);

        appBuffer.compact();
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws IgniteCheckedException {
        GridNioSslHandler hnd = sslHandler(ses);

        hnd.lock();

        try {
            return shutdownSession(ses, hnd);
        }
        finally {
            hnd.unlock();
        }
    }

    /**
     * Sends SSL <tt>close_notify</tt> message and closes underlying TCP connection.
     *
     * @param ses Session to shutdown.
     * @param hnd SSL handler.
     * @return Close future.
     * @throws GridNioException If failed to forward requests to filter chain.
     */
    private GridNioFuture<Boolean> shutdownSession(GridNioSession ses, GridNioSslHandler hnd)
        throws IgniteCheckedException {
        try {
            hnd.closeOutbound();

            hnd.writeNetBuffer(null);
        }
        catch (SSLException e) {
            U.warn(log, "Failed to shutdown SSL session gracefully (will force close) [ex=" + e + ", ses=" + ses + ']');
        }

        return proceedSessionClose(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionIdleTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionWriteTimeout(ses);
    }

    /**
     * Gets ssl handler from the session.
     *
     * @param ses Session instance.
     * @return SSL handler.
     */
    private GridNioSslHandler sslHandler(GridNioSession ses) {
        GridSslMeta sslMeta = ses.meta(SSL_META.ordinal());

        assert sslMeta != null;

        GridNioSslHandler hnd = sslMeta.handler();

        if (hnd == null)
            throw new IgniteException("Failed to process incoming message (received message before SSL handler " +
                "was created): " + ses);

        return hnd;
    }

    /**
     * Checks type of the message passed to the filter and converts it to a byte buffer (since SSL filter
     * operates only on binary data).
     *
     * @param ses Session instance.
     * @param msg Message passed in.
     * @return Message that was cast to a byte buffer.
     * @throws GridNioException If msg is not a byte buffer.
     */
    private ByteBuffer checkMessage(GridNioSession ses, Object msg) throws GridNioException {
        if (!(msg instanceof ByteBuffer))
            throw new GridNioException("Invalid object type received (is SSL filter correctly placed in filter " +
                "chain?) [ses=" + ses + ", msgClass=" + msg.getClass().getName() + ']');

        return (ByteBuffer)msg;
    }
}
