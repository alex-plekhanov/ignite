package org.apache.ignite.internal.processors.platform.client.service;

import org.apache.ignite.internal.processors.platform.client.ClientCloseableResource;

/**
 *
 */
class ClientServiceProxy implements ClientCloseableResource {
    /** Flag keep binary mask. */
    public static final byte FLAG_KEEP_BINARY_MASK = 0x01;

    /** Proxy. */
    private final Object proxy;

    /** Service class. */
    private final Class<?> svcCls;

    /** Flags. */
    private final byte flags;

    /**
     * @param proxy Proxy.
     * @param svcCls Service class.
     * @param flags Flags.
     */
    ClientServiceProxy(Object proxy, Class<?> svcCls, byte flags) {
        this.proxy = proxy;
        this.svcCls = svcCls;
        this.flags = flags;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }

    /**
     *
     */
    public Object proxy() {
        return proxy;
    }

    /**
     *
     */
    public Class<?> svcCls() {
        return svcCls;
    }

    /**
     *
     */
    public byte flags() {
        return flags;
    }
}
