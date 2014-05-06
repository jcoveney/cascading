/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.pipe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Insert;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.BufferJoin;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import static data.InputData.inputFileNums10;

public class CoGroupPlatformTest extends PlatformTestCase
  {
  public CoGroupPlatformTest()
    {
    super( false );
    }

  public static class MultiplyInt extends BaseOperation<Object> implements Function<Object>
    {
    private int multiplier;

    public MultiplyInt(String fieldName, int multiplier)
      {
      super(new Fields(fieldName));
      this.multiplier = multiplier;
      }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Object> functionCall)
      {
      int v = functionCall.getArguments().getTuple().getInteger(0);
      Tuple t = Tuple.size(1);
      t.setInteger(0, v * multiplier);
      functionCall.getOutputCollector().add(t);
      }
    }

  @Test
  public void testCoGroupWithTextDelimited() throws Exception
    {
    getPlatform().copyFromLocal( inputFileNums10 );

    Tap input = getPlatform().getDelimitedFile(
      Fields.ALL, false, "\t", null, null, inputFileNums10, SinkMode.REPLACE );
    Tap intemediate = getPlatform().getDelimitedFile(
      Fields.ALL, false, "\t", null, null, "output/intermediate", SinkMode.REPLACE );
    Tap output = getPlatform().getDelimitedFile(
       Fields.ALL, false, "\t", null, null, "output/final", SinkMode.REPLACE );

    FlowDef flowDef = FlowDef.flowDef().setName("TsvWritingIssue");

    Pipe in1 = new Pipe("in1");
    flowDef = flowDef.addSource(in1, input);

    Pipe in1ToInt = new Pipe("in1ToInt",
      new Each(in1, Fields.FIRST, new MultiplyInt("id", 1), Fields.RESULTS));
    flowDef = flowDef.addTailSink(in1ToInt, intemediate);

    Pipe in2 = new Pipe("in2",
      new Each(in1ToInt, new Fields("id"), new MultiplyInt("id2", 2), Fields.RESULTS));

    Pipe cogroup = new CoGroup(in1ToInt, new Fields("id"), in2, new Fields("id2"));
    flowDef = flowDef.addTailSink(cogroup, output);

    Flow flow = getPlatform().getFlowConnector().connect(flowDef);
    flow.complete();

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "2" ) ) );
    assertTrue( results.contains( new Tuple( "4" ) ) );
    assertTrue( results.contains( new Tuple( "6" ) ) );
    assertTrue( results.contains( new Tuple( "8" ) ) );
    }
  }
