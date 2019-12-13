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

package org.apache.ignite.spi.systemview.view;

/**
 * Utility class for quick iteration over row properties.
 */
public interface SystemViewRowAttributeWalker<R> {
    /** @return Count of a row properties. */
    public int count();

    /**
     * Calls visitor for each row attribute.
     *
     * @param visitor Attribute visitor.
     */
    public void visitAll(AttributeVisitor visitor);

    /**
     * Calls visitor for each row attribute.
     * Value of the attribute also provided.
     *
     * @param row Row to iterate.
     * @param visitor Attribute visitor.
     */
    public void visitAll(R row, AttributeWithValueVisitor visitor);

    /** Attribute visitor. */
    public interface AttributeVisitor {
        /**
         * Visit some object property.
         * @param idx Index.
         * @param name Name.
         * @param clazz Value class.
         * @param filtering Is filtering for this attribute supported.
         * @param <T> Value type.
         */
        public <T> void accept(int idx, String name, Class<T> clazz, boolean filtering);
    }

    /** Attribute visitor. */
    public interface AttributeWithValueVisitor {
        /**
         * Visit attribute. Attribute value is object.
         *
         * @param idx Index.
         * @param name Name.
         * @param clazz Class.
         * @param val Value.
         * @param <T> Value type.
         */
        public <T> void accept(int idx, String name, Class<T> clazz, T val);

        /**
         * Visit attribute. Attribute value is {@code boolean} primitive.
         *
         * @param idx Index.
         * @param name Name.
         * @param val Value.
         */
        public void acceptBoolean(int idx, String name, boolean val);

        /**
         * Visit attribute. Attribute value is {@code char} primitive.
         *
         * @param idx Index.
         * @param name Name.
         * @param val Value.
         */
        public void acceptChar(int idx, String name, char val);

        /**
         * Visit attribute. Attribute value is {@code byte} primitive.
         *
         * @param idx Index.
         * @param name Name.
         * @param val Value.
         */
        public void acceptByte(int idx, String name, byte val);

        /**
         * Visit attribute. Attribute value is {@code short} primitive.
         *
         * @param idx Index.
         * @param name Name.
         * @param val Value.
         */
        public void acceptShort(int idx, String name, short val);

        /**
         * Visit attribute. Attribute value is {@code int} primitive.
         *
         * @param idx Index.
         * @param name Name.
         * @param val Value.
         */
        public void acceptInt(int idx, String name, int val);

        /**
         * Visit attribute. Attribute value is {@code long} primitive.
         *
         * @param idx Index.
         * @param name Name.
         * @param val Value.
         */
        public void acceptLong(int idx, String name, long val);

        /**
         * Visit attribute. Attribute value is {@code float} primitive.
         *
         * @param idx Index.
         * @param name Name.
         * @param val Value.
         */
        public void acceptFloat(int idx, String name, float val);

        /**
         * Visit attribute. Attribute value is {@code double} primitive.
         *
         * @param idx Index.
         * @param name Name.
         * @param val Value.
         */
        public void acceptDouble(int idx, String name, double val);
    }
}
