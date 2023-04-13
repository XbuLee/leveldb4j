/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
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
package cn.leexiaobu.leveldb.impl;

import static cn.leexiaobu.leveldb.impl.LeeDBFactory.asString;
import static cn.leexiaobu.leveldb.impl.LeeDBFactory.bytes;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.leexiaobu.leveldb.api.DB;
import cn.leexiaobu.leveldb.api.DBException;
import cn.leexiaobu.leveldb.api.DBFactory;
import cn.leexiaobu.leveldb.api.Options;
import cn.leexiaobu.leveldb.api.ReadOptions;
import cn.leexiaobu.leveldb.api.WriteOptions;
import cn.leexiaobu.leveldb.util.FileUtils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;


/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class NativeInteropTest {

  private static final AtomicInteger NEXT_ID = new AtomicInteger();

  private final File databaseDir = FileUtils.createTempDir("cn/leexiaobu/leveldb");

  public static void assertEquals(byte[] arg1, byte[] arg2) {
    assertTrue(Arrays.equals(arg1, arg2), asString(arg1) + " != " + asString(arg2));
  }

  private final DBFactory iq80factory = LeeDBFactory.factory;
  private final DBFactory jnifactory;

  public NativeInteropTest() {
    DBFactory jnifactory = LeeDBFactory.factory;
    try {
      ClassLoader cl = NativeInteropTest.class.getClassLoader();
      jnifactory = (DBFactory) cl.loadClass("org.fusesource.leveldbjni.JniDBFactory").newInstance();
    } catch (Throwable e) {
      // We cannot create a JniDBFactory on windows :( so just use a Iq80DBFactory for both
      // to avoid test failures.
    }
    this.jnifactory = jnifactory;
  }

  File getTestDirectory(String name) throws IOException {
    File rc = new File(databaseDir, name);
    iq80factory.destroy(rc, new Options().createIfMissing(true));
    rc.mkdirs();
    return rc;
  }

  @Test
  public void testCRUDviaIQ80() throws IOException, DBException {
    crud(iq80factory, iq80factory);
  }

  @Test
  public void testCRUDviaJNI() throws IOException, DBException {
    crud(jnifactory, jnifactory);
  }

  @Test
  public void testCRUDviaIQ80thenJNI() throws IOException, DBException {
    crud(iq80factory, jnifactory);
  }

  @Test
  public void testCRUDviaJNIthenIQ80() throws IOException, DBException {
    crud(jnifactory, iq80factory);
  }

  public void crud(DBFactory firstFactory, DBFactory secondFactory)
      throws IOException, DBException {
    Options options = new Options().createIfMissing(true);

    File path = getTestDirectory(getClass().getName() + "_" + NEXT_ID.incrementAndGet());
    DB db = firstFactory.open(path, options);

    WriteOptions wo = new WriteOptions().sync(false);
    ReadOptions ro = new ReadOptions().fillCache(true).verifyChecksums(true);
    db.put(bytes("Tampa"), bytes("green"));
    db.put(bytes("London"), bytes("red"));
    db.put(bytes("New York"), bytes("blue"));

    db.close();
    db = secondFactory.open(path, options);

    assertEquals(db.get(bytes("Tampa"), ro), bytes("green"));
    assertEquals(db.get(bytes("London"), ro), bytes("red"));
    assertEquals(db.get(bytes("New York"), ro), bytes("blue"));

    db.delete(bytes("New York"), wo);

    assertEquals(db.get(bytes("Tampa"), ro), bytes("green"));
    assertEquals(db.get(bytes("London"), ro), bytes("red"));
    assertNull(db.get(bytes("New York"), ro));

    db.close();
    db = firstFactory.open(path, options);

    assertEquals(db.get(bytes("Tampa"), ro), bytes("green"));
    assertEquals(db.get(bytes("London"), ro), bytes("red"));
    assertNull(db.get(bytes("New York"), ro));

    db.close();
  }
}
