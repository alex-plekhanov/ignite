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

namespace Apache.Ignite.Core.Configuration
{
    using System.ComponentModel;

    /// <summary>
    /// Server-side thin client specific configuration.
    /// </summary>
    public class ThinClientConfiguration
    {
        /// <summary>
        /// Default limit of active transactions count per connection.
        /// </summary>
        public const int DefaultMaxActiveTxPerConnection = 100;

        /// <summary>
        /// Default value of compute enabled flag.
        /// </summary>
        public const bool DefaultComputeEnabled = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="ThinClientConfiguration"/> class.
        /// </summary>
        public ThinClientConfiguration()
        {
            MaxActiveTxPerConnection = DefaultMaxActiveTxPerConnection;
        }

        /// <summary>
        /// Gets or sets active transactions count per connection limit.
        /// </summary>
        [DefaultValue(DefaultMaxActiveTxPerConnection)]
        public int MaxActiveTxPerConnection { get; set; }

        /// <summary>
        /// Gets or sets compute enabled flag.
        /// </summary>
        [DefaultValue(DefaultComputeEnabled)]
        public bool ComputeEnabled { get; set; }
    }
}
