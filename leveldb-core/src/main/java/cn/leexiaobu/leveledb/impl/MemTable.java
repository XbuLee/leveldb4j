package cn.leexiaobu.leveledb.impl;

import static cn.leexiaobu.leveledb.util.SizeOf.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

import cn.leexiaobu.leveledb.util.InternalIterator;
import cn.leexiaobu.leveledb.util.Slice;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Leexiaobu
 * @date 2023-03-07 14:28
 */
public class MemTable implements SeekingIterable<InternalKey, Slice> {

  //自带的跳表结构
  private final ConcurrentSkipListMap<InternalKey, Slice> table;
  private final AtomicLong approximateMemoryUsage = new AtomicLong();

  public MemTable(InternalKeyComparator internalKeyComparator) {
    table = new ConcurrentSkipListMap<>(internalKeyComparator);
  }

  public boolean isEmpty() {
    return table.isEmpty();
  }

  public long approximateMemoryUsage() {
    return approximateMemoryUsage.get();
  }

  public void add(long sequenceNumber, ValueType valueType, Slice key, Slice value) {
    requireNonNull(valueType, "valueType is null");
    requireNonNull(key, "key is null");
    requireNonNull(valueType, "valueType is null");
    InternalKey internalKey = new InternalKey(key, sequenceNumber, valueType);
    table.put(internalKey, value);
    approximateMemoryUsage.addAndGet(key.length() + SIZE_OF_LONG + value.length());
  }

  public LookupResult get(LookupKey key) {
    requireNonNull(key, "key is null");
    InternalKey internalKey = key.getInternalKey();
    //返回大于或者等于给定key的最小值,返回的entry不支持setValue
    //fixme
    // 1.为什么取天花板值, 是因为不好判断是否存在,所以需要遍历到skiplist的最底层吗
    // 2.同时为什么不去地板值
    // 3.怎么不在文件的ssTable中查找
    Entry<InternalKey, Slice> entry = table.ceilingEntry(internalKey);
    if (entry == null) {
      return null;
    }
    InternalKey entryKey = entry.getKey();
    if (entryKey.getUserKey().equals(key.getUserKey())) {
      if (entryKey.getValueType() == ValueType.DELETION) {
        return LookupResult.deleted(key);
      } else {
        return LookupResult.ok(key, entry.getValue());
      }
    }
    return null;
  }

  @Override
  public MemTableIterator iterator() {
    return new MemTableIterator();
  }

  public class MemTableIterator implements InternalIterator {

    private PeekingIterator<Entry<InternalKey, Slice>> iterator;

    public MemTableIterator() {
      iterator = Iterators.peekingIterator(table.entrySet().iterator());
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public void seekToFirst() {
      iterator = Iterators.peekingIterator(table.entrySet().iterator());
    }

    @Override
    public void seek(InternalKey targetKey) {
      iterator = Iterators.peekingIterator(table.tailMap(targetKey).entrySet().iterator());
    }

    @Override
    public InternalEntry peek() {
      Entry<InternalKey, Slice> entry = iterator.peek();
      return new InternalEntry(entry.getKey(), entry.getValue());
    }

    @Override
    public InternalEntry next() {
      Entry<InternalKey, Slice> entry = iterator.next();
      return new InternalEntry(entry.getKey(), entry.getValue());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}