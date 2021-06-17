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

package org.apache.ignite.examples.ml.inference.spark.modelparser;

import java.io.FileNotFoundException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.tutorial.TitanicUtils;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.environment.logging.ConsoleLogger;
import org.apache.ignite.ml.environment.parallelism.ParallelismStrategy;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.apache.ignite.ml.sparkmodelparser.SparkModelParser;
import org.apache.ignite.ml.sparkmodelparser.SupportedSparkModels;

/**
 * Run Random Forest model loaded from snappy.parquet file. The snappy.parquet file was generated by Spark MLLib
 * model.write.overwrite().save(..) operator.
 * <p>
 * You can change the test data used in this example and re-run it to explore this algorithm further.</p>
 */
public class RandomForestFromSparkExample {
    /**
     * Path to Spark Random Forest model.
     */
    public static final String SPARK_MDL_PATH = "examples/src/main/resources/models/spark/serialized/rf";

    /** Learning environment. */
    public static final LearningEnvironment env =
        LearningEnvironmentBuilder.defaultBuilder().withParallelismStrategyTypeDependency(ParallelismStrategy.ON_DEFAULT_POOL)
            .withLoggingFactoryDependency(ConsoleLogger.Factory.HIGH).buildForTrainer();

    /**
     * Run example.
     */
    public static void main(String[] args) throws FileNotFoundException {
        System.out.println();
        System.out.println(
            ">>> Random Forest model loaded from Spark through serialization over partitioned dataset usage example started."
        );
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            try {
                dataCache = TitanicUtils.readPassengersWithoutNulls(ignite);

                final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>(0, 5, 6).labeled(1);

                ModelsComposition mdl = (ModelsComposition)SparkModelParser.parse(
                    SPARK_MDL_PATH,
                    SupportedSparkModels.RANDOM_FOREST,
                    env
                );

                System.out.println(">>> Random Forest model: " + mdl.toString(true));

                double accuracy = Evaluator.evaluate(
                    dataCache,
                    mdl,
                    vectorizer,
                    new Accuracy<>()
                );

                System.out.println("\n>>> Accuracy " + accuracy);
                System.out.println("\n>>> Test Error " + (1 - accuracy));
            }
            finally {
                dataCache.destroy();
            }
        }
    }
}
