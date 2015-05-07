/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.demos.dimensions.sales.generic;

import com.datatorrent.lib.appdata.dimensions.DimensionsComputationSingleSchemaMap;
import com.datatorrent.lib.appdata.dimensions.DimensionsMapConverter;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import org.junit.Test;

import static com.datatorrent.demos.dimensions.ads.generic.AdsDimensionsDemo.EVENT_SCHEMA;

public class SalesDemoWithHDHTTest
{
  @Test
  public void salesDemoWithHDHT()
  {
    DimensionsComputationSingleSchemaMap dcss =
    new DimensionsComputationSingleSchemaMap();

    dcss.setEventSchemaJSON(SchemaUtils.jarResourceFileToString(EVENT_SCHEMA));
    dcss.setConverter(new DimensionsMapConverter());

    dcss.setup(null);
  }
}