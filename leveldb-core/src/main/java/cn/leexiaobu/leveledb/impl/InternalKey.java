package cn.leexiaobu.leveledb.impl;

import static cn.leexiaobu.leveledb.util.SizeOf.SIZE_OF_LONG;
import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import cn.leexiaobu.leveledb.util.Slice;
import cn.leexiaobu.leveledb.util.SliceOutput;
import cn.leexiaobu.leveledb.util.Slices;
import java.util.Objects;

/**
 * @author Leexiaobu
 * @date 2023-03-07 14:29
 */
public class InternalKey {


  private final Slice userKey;
  private final long sequenceNumber;
  private final ValueType valueType;
  private int hash;

  public InternalKey(Slice userKey, long sequenceNumber, ValueType valueType) {
    requireNonNull(userKey, "userKey is null");
    checkArgument(sequenceNumber >= 0, "sequenceNumber is negative");
    requireNonNull(valueType, "Value type is null");
    this.userKey = userKey;
    this.sequenceNumber = sequenceNumber;
    this.valueType = valueType;
  }

  public InternalKey(Slice data) {
    requireNonNull(data, "data is null");
    checkArgument(data.length() >= SIZE_OF_LONG, "data must be at least %s bytes", SIZE_OF_LONG);
    this.userKey = getUserKey(data);
    long packedSequenceAndType = data.getLong(data.length() - SIZE_OF_LONG);
    this.sequenceNumber = SequenceNumber.unpackSequenceNumber(packedSequenceAndType);
    this.valueType = SequenceNumber.unpackValueType(packedSequenceAndType);
  }

  private static Slice getUserKey(Slice data) {
    //seq7 + valueType1 =8
    return data.slice(0, data.length() - SIZE_OF_LONG);
  }

  public Slice getUserKey() {
    return userKey;
  }

  public ValueType getValueType() {
    return valueType;
  }

  public long getSequenceNumber() {
    return sequenceNumber;
  }

  public Slice encode() {
    Slice slice = Slices.allocate(userKey.length() + SIZE_OF_LONG);
    SliceOutput sliceOutput = slice.output();
    sliceOutput.writeBytes(userKey);
    sliceOutput.writeLong(SequenceNumber.packSequenceAndValueType(sequenceNumber, valueType));
    return slice;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InternalKey that = (InternalKey) o;

    if (sequenceNumber != that.sequenceNumber) {
      return false;
    }
    if (!Objects.equals(userKey, that.userKey)) {
      return false;
    }
    return valueType == that.valueType;
  }

  @Override
  public int hashCode() {
    if (hash == 0) {
      int result = userKey != null ? userKey.hashCode() : 0;
      result = 31 * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
      result = 31 * result + (valueType != null ? valueType.hashCode() : 0);
      if (result == 0) {
        result = 1;
      }
      hash = result;
    }
    return hash;
  }

  @Override
  public String toString() {
    String sb = "InternalKey"
        + "{key=" + getUserKey().toString(UTF_8)      // todo don't print the real value
        + ", sequenceNumber=" + getSequenceNumber()
        + ", valueType=" + getValueType()
        + '}';
    return sb;
  }

}