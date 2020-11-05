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

package org.apache.ignite.internal.processors.service;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceDescriptor;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** */
public class ServiceWithDeploymentSpiTest extends GridCommonAbstractTest {
    /** */
    private Path srcTmpDir;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDeploymentSpi(new LocalDeploymentSpi());

        return cfg;
    }

    /** */
    @Before
    public void prepare() throws IOException {
        srcTmpDir = Files.createTempDirectory(getClass().getSimpleName());
    }

    /** */
    @After
    public void cleanup() {
        U.delete(srcTmpDir);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServiceWithDeploymentSpi() throws Exception {
        URLClassLoader clsLdr = prepareClassLoader();

        Class<?> cls = clsLdr.loadClass("MyServiceImpl");

        Service srvc = (Service)cls.newInstance();

        Ignite ignite = startGrid(0);

        DeploymentSpi depSpi = ignite.configuration().getDeploymentSpi();

        depSpi.register(clsLdr, srvc.getClass());

        ignite.services().deployClusterSingleton("test-service", srvc);

        for (ServiceDescriptor desc : ignite.services().serviceDescriptors()) {
            if ("test-service".equals(desc.name()))
                assertEquals(cls, desc.serviceClass());
        }
    }

    /** */
    private URLClassLoader prepareClassLoader() throws Exception {
        String src = "import org.apache.ignite.services.Service;\n" +
            "import org.apache.ignite.services.ServiceContext;\n" +
            "public class MyServiceImpl implements Service {\n" +
            "    @Override public void cancel(ServiceContext ctx) {}\n" +
            "    @Override public void init(ServiceContext ctx) throws Exception {}\n" +
            "    @Override public void execute(ServiceContext ctx) throws Exception {}\n" +
            "}";

        Files.createDirectories(srcTmpDir);

        File srcFile = new File(srcTmpDir.toFile(), "MyServiceImpl.java");

        Path srcFilePath = Files.write(srcFile.toPath(), src.getBytes(StandardCharsets.UTF_8));

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

        compiler.run(null, null, null, srcFilePath.toString());

        assertTrue("Failed to remove source file.", srcFile.delete());

        return new URLClassLoader(new URL[] {srcTmpDir.toUri().toURL()});
    }
}
