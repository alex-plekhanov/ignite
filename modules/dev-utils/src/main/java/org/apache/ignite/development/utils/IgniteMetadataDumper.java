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

package org.apache.ignite.development.utils;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class IgniteMetadataDumper {
    /** */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: IgniteMetadataDumper pathToMetadata");
            return;
        }

        File path = new File(args[0]);

        for (File cluster : path.listFiles()) {
            for (File node : cluster.listFiles())
                dumpMeta(node);
        }
    }

    /** */
    private static void dumpMeta(File path) throws Exception {
        NodeFileTree ft = new NodeFileTree(path, "None");

        GridKernalContext ctx = new StandaloneGridKernalContext(IgniteWalIteratorFactory.ConsoleLogger.INSTANCE, ft);

        BinaryMarshaller marshaller = ctx.marshaller();
        marshaller.setBinaryContext(U.binaryContext(marshaller));

        for (File file : path.listFiles(f -> f.getName().endsWith(".bin"))) {
            try (FileInputStream in = new FileInputStream(file)) {
                BinaryMetadata meta = U.unmarshal(marshaller, in, U.resolveClassLoader(ctx.config()));
                Path dumpFile = Path.of(file.getAbsolutePath() + ".txt");
                U.delete(dumpFile);
                Files.writeString(dumpFile, format(meta));
            }
            catch (Exception e) {
                IgniteWalIteratorFactory.ConsoleLogger.INSTANCE.error("Exception", e);
            }
        }
    }

    /** */
    private static String format(BinaryMetadata meta) {
        SB sb = new SB();
        sb.a("typeId=").a(meta.typeId()).nl();
        sb.a("typeName=").a(meta.typeName()).nl();
        sb.a("fields="); mapToSb(meta.fieldsMap(), sb);
        sb.a("affKeyFieldName=").a(meta.affinityKeyFieldName()).nl();
        sb.a("isEnum=").a(meta.isEnum()).nl();
        sb.a("enumValues=");mapToSb(meta.enumMap(), sb);

        Map<Integer, String> flds = new HashMap<>();
        for (Map.Entry<String, BinaryFieldMetadata> e : meta.fieldsMap().entrySet())
            flds.compute(e.getValue().fieldId(), (k, v) -> v != null && v.compareTo(e.getKey()) >= 0 ? v : e.getKey());

        sb.a("schemas=");
        collectionToSb(meta.schemas(), sb, (sb0, val) -> sb0.a("schemaId=").a(val.schemaId()).a(", ").a("fieldNames=")
            .a('[').a(Arrays.stream(val.fieldIds()).mapToObj(flds::get).collect(Collectors.joining(", "))).a(']'));

        return sb.toString();
    }

    /** */
    private static void mapToSb(Map<?, ?> map, SB sb) {
        sb.a('[').nl();

        for (Map.Entry<?, ?> e : map.entrySet())
            sb.a("    ").a(e.getKey()).a('=').a(e.getValue()).nl();

        sb.a(']').nl();
    }

    /** */
    private static <T> void collectionToSb(Collection<T> c, SB sb, BiConsumer<SB, T> consumer) {
        sb.a('[').nl();

        for (T val : c) {
            sb.a("    ");
            consumer.accept(sb, val);
            sb.nl();
        }

        sb.a(']').nl();
    }
}
