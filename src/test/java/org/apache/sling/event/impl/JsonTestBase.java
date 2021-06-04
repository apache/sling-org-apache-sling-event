package org.apache.sling.event.impl;


import org.apache.felix.inventory.Format;
import org.apache.felix.inventory.InventoryPrinter;
import org.mockito.Mockito;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import javax.json.Json;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;

public abstract class JsonTestBase extends Mockito {

    protected String queryInventoryJSON(InventoryPrinter inventoryPrinter, boolean isZip) throws Exception {
        StringWriter writerOutput = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writerOutput);

        inventoryPrinter.print(printWriter, Format.JSON, false);

        String jsonString = writerOutput.toString();

        return Json.createReader(new StringReader(jsonString)).readObject().toString();
    }

    protected String queryInventoryJSON(InventoryPrinter inventoryPrinter) throws Exception {
        return queryInventoryJSON(inventoryPrinter, false);
    }
}
