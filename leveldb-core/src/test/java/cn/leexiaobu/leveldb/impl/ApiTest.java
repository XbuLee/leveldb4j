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
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.leexiaobu.leveldb.api.CompressionType;
import cn.leexiaobu.leveldb.api.DB;
import cn.leexiaobu.leveldb.api.DBException;
import cn.leexiaobu.leveldb.api.DBFactory;
import cn.leexiaobu.leveldb.api.Options;
import cn.leexiaobu.leveldb.util.FileUtils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.junit.jupiter.api.Test;


/**
 * Test the implemenation via the cn.leexiaobu.leveldb API.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ApiTest {

  private final File databaseDir = FileUtils.createTempDir("cn/leexiaobu/leveldb");

  public void assertEquals(byte[] arg1, byte[] arg2) {
    assertTrue(Arrays.equals(arg1, arg2), asString(arg1) + " != " + asString(arg2));
  }

  private final DBFactory factory = LeeDBFactory.factory;

  File getTestDirectory(String name) throws IOException {
    File rc = new File(databaseDir, name);
    factory.destroy(rc, new Options().createIfMissing(true));
    rc.mkdirs();
    return rc;
  }

  @Test
  public void testCompaction() throws IOException, DBException {
    Options options = new Options().createIfMissing(true).compressionType(CompressionType.NONE);
    File path = getTestDirectory("testCompaction");
    DB db = factory.open(path, options);

    System.out.println("Adding");
    for (int i = 0; i < 1000 * 1000; i++) {
      if (i % 100000 == 0) {
        System.out.println("  at: " + i);
      }
      db.put(bytes("key" + i), bytes("value" + i));
    }

    db.close();
    db = factory.open(path, options);

    System.out.println("Deleting");
    for (int i = 0; i < 1000 * 1000; i++) {
      if (i % 100000 == 0) {
        System.out.println("  at: " + i);
      }
      db.delete(bytes("key" + i));
    }

    db.close();
    db = factory.open(path, options);

    System.out.println("Adding");
    for (int i = 0; i < 1000 * 1000; i++) {
      if (i % 100000 == 0) {
        System.out.println("  at: " + i);
      }
      db.put(bytes("key" + i), bytes("value" + i));
    }

    db.close();
  }
}
