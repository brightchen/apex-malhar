/**
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
package org.apache.apex.malhar.lib.window.accumulation;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for POJO outer join accumulations
 */
public class PojoOuterJoinTest
{

  public static class TestPojo1
  {
    private int uId;
    private String uName;

    public TestPojo1()
    {

    }

    public TestPojo1(int id, String name)
    {
      this.uId = id;
      this.uName = name;
    }

    public int getUId()
    {
      return uId;
    }

    public void setUId(int uId)
    {
      this.uId = uId;
    }

    public String getUName()
    {
      return uName;
    }

    public void setUName(String uName)
    {
      this.uName = uName;
    }
  }

  public static class TestPojo3
  {
    private int uId;
    private String uNickName;
    private int age;

    public TestPojo3()
    {

    }

    public TestPojo3(int id, String name, int age)
    {
      this.uId = id;
      this.uNickName = name;
      this.age = age;
    }

    public int getUId()
    {
      return uId;
    }

    public void setUId(int uId)
    {
      this.uId = uId;
    }

    public String getUNickName()
    {
      return uNickName;
    }

    public void setUNickName(String uNickName)
    {
      this.uNickName = uNickName;
    }

    public int getAge()
    {
      return age;
    }

    public void setAge(int age)
    {
      this.age = age;
    }
  }


  public static class TestOutClass
  {
    private int uId;
    private String uName;
    private String uNickName;
    private int age;

    public int getUId()
    {
      return uId;
    }

    public void setUId(int uId)
    {
      this.uId = uId;
    }

    public String getUName()
    {
      return uName;
    }

    public void setUName(String uName)
    {
      this.uName = uName;
    }

    public String getUNickName()
    {
      return uNickName;
    }

    public void setUNickName(String uNickName)
    {
      this.uNickName = uNickName;
    }

    public int getAge()
    {
      return age;
    }

    public void setAge(int age)
    {
      this.age = age;
    }
  }

  public static class TestOutMultipleKeysClass
  {
    private int uId;
    private String uName;
    private int age;

    public int getUId()
    {
      return uId;
    }

    public void setUId(int uId)
    {
      this.uId = uId;
    }

    public String getUName()
    {
      return uName;
    }

    public void setUName(String uName)
    {
      this.uName = uName;
    }

    public int getAge()
    {
      return age;
    }

    public void setAge(int age)
    {
      this.age = age;
    }
  }


  @Test
  public void PojoLeftOuterJoinTest()
  {
    PojoLeftOuterJoin<TestPojo1, TestPojo3> pij = new PojoLeftOuterJoin<>(2, TestOutClass.class, "uId", "uId");

    List<List<Map<String, Object>>> accu = pij.defaultAccumulatedValue();

    Assert.assertEquals(2, accu.size());

    accu = pij.accumulate(accu, new TestPojo1(1, "Josh"));
    accu = pij.accumulate(accu, new TestPojo1(2, "Bob"));

    accu = pij.accumulate2(accu, new TestPojo3(1, "NickJosh", 12));
    accu = pij.accumulate2(accu, new TestPojo3(3, "NickBob", 13));

    Assert.assertEquals(2, pij.getOutput(accu).size());

    Object o = pij.getOutput(accu).get(1);
    Assert.assertTrue(o instanceof TestOutClass);
    TestOutClass testOutClass = (TestOutClass)o;
    Assert.assertEquals(2, testOutClass.getUId());
    Assert.assertEquals("Bob", testOutClass.getUName());
    Assert.assertEquals(0, testOutClass.getAge());
  }

  @Test
  public void PojoRightOuterJoinTest()
  {
    PojoRightOuterJoin<TestPojo1, TestPojo3> pij = new PojoRightOuterJoin<>(2, TestOutClass.class, "uId", "uId");

    List<List<Map<String, Object>>> accu = pij.defaultAccumulatedValue();

    Assert.assertEquals(2, accu.size());

    accu = pij.accumulate(accu, new TestPojo1(1, "Josh"));
    accu = pij.accumulate(accu, new TestPojo1(2, "Bob"));

    accu = pij.accumulate2(accu, new TestPojo3(1, "NickJosh", 12));
    accu = pij.accumulate2(accu, new TestPojo3(3, "NickBob", 13));

    Assert.assertEquals(2, pij.getOutput(accu).size());

    Object o = pij.getOutput(accu).get(1);
    Assert.assertTrue(o instanceof TestOutClass);
    TestOutClass testOutClass = (TestOutClass)o;
    Assert.assertEquals(3, testOutClass.getUId());
    Assert.assertEquals(null, testOutClass.getUName());
    Assert.assertEquals(13, testOutClass.getAge());
  }

  @Test
  public void PojoFullOuterJoinTest()
  {
    PojoFullOuterJoin<TestPojo1, TestPojo3> pij = new PojoFullOuterJoin<>(2, TestOutClass.class, "uId", "uId");

    List<List<Map<String, Object>>> accu = pij.defaultAccumulatedValue();

    Assert.assertEquals(2, accu.size());

    accu = pij.accumulate(accu, new TestPojo1(1, "Josh"));
    accu = pij.accumulate(accu, new TestPojo1(2, "Bob"));

    accu = pij.accumulate2(accu, new TestPojo3(1, "NickJosh", 12));
    accu = pij.accumulate2(accu, new TestPojo3(3, "NickBob", 13));

    Assert.assertEquals(3, pij.getOutput(accu).size());

    Object o = pij.getOutput(accu).get(1);
    Assert.assertTrue(o instanceof TestOutClass);
    TestOutClass testOutClass = (TestOutClass)o;
    Assert.assertEquals(2, testOutClass.getUId());
    Assert.assertEquals("Bob", testOutClass.getUName());
    Assert.assertEquals(0, testOutClass.getAge());
  }
}
