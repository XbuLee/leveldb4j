package cn.leexiaobu.leveldb.util;

import static cn.leexiaobu.leveldb.util.SizeOf.SIZE_OF_BYTE;
import static cn.leexiaobu.leveldb.util.SizeOf.SIZE_OF_INT;
import static cn.leexiaobu.leveldb.util.SizeOf.SIZE_OF_LONG;
import static cn.leexiaobu.leveldb.util.SizeOf.SIZE_OF_SHORT;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.checkFromToIndex;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * @author Leexiaobu
 * @date 2023-03-06 15:19
 */
public class Slice implements Comparable<Slice> {

  private final byte[] data;
  private final int offset;
  private final int length;

  private int hash;

  public Slice(int length) {
    data = new byte[length];
    this.offset = 0;
    this.length = length;
  }

  public Slice(byte[] data, int offset, int length) {
    requireNonNull(data, "array is null");
    this.data = data;
    this.offset = offset;
    this.length = length;
  }

  public Slice(byte[] data) {
    requireNonNull(data, "array is null");
    this.data = data;
    this.offset = 0;
    this.length = data.length;
  }

  public Slice slice() {
    return slice(0, length);
  }

  public Slice slice(int index, int length) {
    if (index == 0 && length == this.length) {
      return this;
    }

    checkPositionIndexes(index, index + length, this.length);
    if (index >= 0 && length == 0) {
      return Slices.EMPTY_SLICE;
    }
    return new Slice(data, offset + index, length);
  }

  public byte[] getRawArray() {
    return data;
  }


  public int getRawOffset() {
    return offset;
  }

  public byte getByte(int index) {
    checkFromToIndex(index, index + SIZE_OF_BYTE, this.length);
    index += offset;
    return data[index];
  }

  public short getUnsignedByte(int index) {
    return (short) (getByte(index) & 0xFF);
  }

  public short getShort(int index) {
    checkPositionIndexes(index, index + SIZE_OF_SHORT, this.length);
    index += offset;
    return (short) (data[index] & 0xFF | data[index + 1] << 8);
  }

  public int getInt(int index) {
    checkFromToIndex(index, index + SIZE_OF_INT, this.length);
    index += offset;
    return data[index] & 0xFF | (data[index + 1] & 0xFF) << 8
        | (data[index + 2] & 0xFF) << 16 | (data[index + 3] & 0xFF) << 24;
  }

  public long getLong(int index) {
    checkFromToIndex(index, index + SIZE_OF_LONG, this.length);
    index += offset;
    return (long) data[index] & 0xFF | ((long) data[index + 1] & 0xFF) << 8
        | ((long) data[index + 2] & 0xFF) << 16 | ((long) data[index + 3] & 0xFF) << 24
        | ((long) data[index + 4] & 0xFF) << 32 | ((long) data[index + 5] & 0xFF) << 40
        | ((long) data[index + 6] & 0xFF) << 48 | ((long) data[index + 7] & 0xFF) << 56;
  }

  /**
   * 转移数据data
   */
  public void getBytes(int index, Slice dst, int dstIndex, int length) {
    getBytes(index, dst.data, dstIndex, length);
  }

  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    checkPositionIndexes(index, index + length, this.length);
    index += offset;
    return out.write(ByteBuffer.wrap(data, index, length));
  }

  public void getBytes(int index, OutputStream out, int length) throws IOException {
    checkPositionIndexes(index, index + length, this.length);
    index += offset;
    out.write(data, index, length);
  }

  public void getBytes(int index, byte[] destination, int destinationIndex, int length) {
    checkPositionIndexes(index, index + length, this.length);
    checkPositionIndexes(destinationIndex, destinationIndex + length, destination.length);
    index += offset;
    System.arraycopy(data, index, destination, destinationIndex, length);
  }

  public byte[] getBytes() {
    return getBytes(0, length);
  }

  public byte[] getBytes(int index, int length) {
    index += offset;
    if (index == 0) {
      return Arrays.copyOf(data, length);
    } else {
      byte[] value = new byte[length];
      System.arraycopy(data, index, value, 0, length);
      return value;
    }
  }

  public void getBytes(int index, ByteBuffer destination) {
    checkPositionIndex(index, this.length);
    index += offset;
    destination.put(data, index, Math.min(length, destination.remaining()));
  }

  /**
   * Sets the specified 16-bit short integer at the specified absolute {@code index} in this buffer.
   * The 16 high-order bits of the specified value are ignored.
   *
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
   * {@code index + 2} is greater than {@code this.capacity}
   */
  public void setShort(int index, int value) {
    checkPositionIndexes(index, index + SIZE_OF_SHORT, this.length);
    index += offset;
    data[index] = (byte) (value);
    data[index + 1] = (byte) (value >>> 8);
  }

  /**
   * Sets the specified 32-bit integer at the specified absolute {@code index} in this buffer.
   *
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
   * {@code index + 4} is greater than {@code this.capacity}
   */
  public void setInt(int index, int value) {
    checkPositionIndexes(index, index + SIZE_OF_INT, this.length);
    index += offset;
    data[index] = (byte) (value);
    data[index + 1] = (byte) (value >>> 8);
    data[index + 2] = (byte) (value >>> 16);
    data[index + 3] = (byte) (value >>> 24);
  }

  /**
   * Sets the specified 64-bit long integer at the specified absolute {@code index} in this buffer.
   *
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
   * {@code index + 8} is greater than {@code this.capacity}
   */
  public void setLong(int index, long value) {
    checkPositionIndexes(index, index + SIZE_OF_LONG, this.length);
    index += offset;
    data[index] = (byte) (value);
    data[index + 1] = (byte) (value >>> 8);
    data[index + 2] = (byte) (value >>> 16);
    data[index + 3] = (byte) (value >>> 24);
    data[index + 4] = (byte) (value >>> 32);
    data[index + 5] = (byte) (value >>> 40);
    data[index + 6] = (byte) (value >>> 48);
    data[index + 7] = (byte) (value >>> 56);
  }

  /**
   * Sets the specified byte at the specified absolute {@code index} in this buffer.  The 24
   * high-order bits of the specified value are ignored.
   *
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
   * {@code index + 1} is greater than {@code this.capacity}
   */
  public void setByte(int index, int value) {
    checkPositionIndexes(index, index + SIZE_OF_BYTE, this.length);
    index += offset;
    data[index] = (byte) value;
  }

  /**
   * Transfers the specified source buffer's data to this buffer starting at the specified absolute
   * {@code index}.
   *
   * @param srcIndex the first index of the source
   * @param length the number of bytes to transfer
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, if the
   * specified {@code srcIndex} is less than {@code 0}, if {@code index + length} is greater than
   * {@code this.capacity}, or if {@code srcIndex + length} is greater than {@code src.capacity}
   */
  public void setBytes(int index, Slice src, int srcIndex, int length) {
    setBytes(index, src.data, src.offset + srcIndex, length);
  }

  /**
   * Transfers the specified source array's data to this buffer starting at the specified absolute
   * {@code index}.
   *
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, if the
   * specified {@code srcIndex} is less than {@code 0}, if {@code index + length} is greater than
   * {@code this.capacity}, or if {@code srcIndex + length} is greater than {@code src.length}
   */
  public void setBytes(int index, byte[] source, int sourceIndex, int length) {
    checkPositionIndexes(index, index + length, this.length);
    checkPositionIndexes(sourceIndex, sourceIndex + length, source.length);
    index += offset;
    System.arraycopy(source, sourceIndex, data, index, length);
  }

  /**
   * Transfers the specified source buffer's data to this buffer starting at the specified absolute
   * {@code index} until the source buffer's position reaches its limit.
   *
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or if
   * {@code index + src.remaining()} is greater than {@code this.capacity}
   */
  public void setBytes(int index, ByteBuffer source) {
    checkPositionIndexes(index, index + source.remaining(), this.length);
    index += offset;
    source.get(data, index, source.remaining());
  }

  /**
   * Transfers the content of the specified source stream to this buffer starting at the specified
   * absolute {@code index}.
   *
   * @param length the number of bytes to transfer
   * @return the actual number of bytes read in from the specified channel. {@code -1} if the
   * specified channel is closed.
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or if
   * {@code index + length} is greater than {@code this.capacity}
   * @throws java.io.IOException if the specified stream threw an exception during I/O
   */
  public int setBytes(int index, InputStream in, int length) throws IOException {
    checkPositionIndexes(index, index + length, this.length);
    index += offset;
    int readBytes = 0;
    do {
      int localReadBytes = in.read(data, index, length);
      if (localReadBytes < 0) {
        if (readBytes == 0) {
          return -1;
        } else {
          break;
        }
      }
      readBytes += localReadBytes;
      index += localReadBytes;
      length -= localReadBytes;
    } while (length > 0);

    return readBytes;
  }

  /**
   * Transfers the content of the specified source channel to this buffer starting at the specified
   * absolute {@code index}.
   *
   * @param length the maximum number of bytes to transfer
   * @return the actual number of bytes read in from the specified channel. {@code -1} if the
   * specified channel is closed.
   * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or if
   * {@code index + length} is greater than {@code this.capacity}
   * @throws java.io.IOException if the specified channel threw an exception during I/O
   */
  public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
    checkPositionIndexes(index, index + length, this.length);
    index += offset;
    ByteBuffer buf = ByteBuffer.wrap(data, index, length);
    int readBytes = 0;

    do {
      int localReadBytes;
      try {
        localReadBytes = in.read(buf);
      } catch (ClosedChannelException e) {
        localReadBytes = -1;
      }
      if (localReadBytes < 0) {
        if (readBytes == 0) {
          return -1;
        } else {
          break;
        }
      } else if (localReadBytes == 0) {
        break;
      }
      readBytes += localReadBytes;
    } while (readBytes < length);

    return readBytes;
  }

  public int setBytes(int index, FileChannel in, int position, int length) throws IOException {
    checkPositionIndexes(index, index + length, this.length);
    index += offset;
    ByteBuffer buf = ByteBuffer.wrap(data, index, length);
    int readBytes = 0;

    do {
      int localReadBytes;
      try {
        localReadBytes = in.read(buf, position + readBytes);
      } catch (ClosedChannelException e) {
        localReadBytes = -1;
      }
      if (localReadBytes < 0) {
        if (readBytes == 0) {
          return -1;
        } else {
          break;
        }
      } else if (localReadBytes == 0) {
        break;
      }
      readBytes += localReadBytes;
    } while (readBytes < length);

    return readBytes;
  }

  public Slice copySlice() {
    return copySlice(0, length);
  }

  /**
   * Returns a copy of this buffer's sub-region.  Modifying the content of the returned buffer or
   * this buffer does not affect each other at all.
   */
  public Slice copySlice(int index, int length) {
    checkPositionIndexes(index, index + length, this.length);

    index += offset;
    byte[] copiedArray = new byte[length];
    System.arraycopy(data, index, copiedArray, 0, length);
    return new Slice(copiedArray);
  }

  public byte[] copyBytes() {
    return copyBytes(0, length);
  }

  public byte[] copyBytes(int index, int length) {
    checkPositionIndexes(index, index + length, this.length);
    index += offset;
    if (index == 0) {
      return Arrays.copyOf(data, length);
    } else {
      byte[] value = new byte[length];
      System.arraycopy(data, index, value, 0, length);
      return value;
    }
  }

  public ByteBuffer toByteBuffer() {
    return toByteBuffer(0, length);
  }

  /**
   * Converts this buffer's sub-region into a NIO buffer.  The returned buffer shares the content
   * with this buffer.
   */
  public ByteBuffer toByteBuffer(int index, int length) {
    checkPositionIndexes(index, index + length, this.length);
    index += offset;
    return ByteBuffer.wrap(data, index, length).order(LITTLE_ENDIAN);
  }

  public int length() {
    return length;
  }

  public int getHash() {
    return hash;
  }

  /**
   * Creates an input stream over this slice.
   */
  public SliceInput input() {
    return new SliceInput(this);
  }

  /**
   * Creates an output stream over this slice.
   */
  public SliceOutput output() {
    return new BasicSliceOutput(this);
  }


  @Override
  //fixme equalsweihsnem1bu duibi1
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Slice slice = (Slice) o;

    // do lengths match
    if (length != slice.length) {
      return false;
    }

    // if arrays have same base offset, some optimizations can be taken...
    if (offset == slice.offset && data == slice.data) {
      return true;
    }
    for (int i = 0; i < length; i++) {
      if (data[offset + i] != slice.data[slice.offset + i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    if (hash != 0) {
      return hash;
    }
    int result = length;
    for (int i = offset; i < offset + length; i++) {
      result = 31 * result + data[i];
    }
    if (result == 0) {
      result = 1;
    }
    hash = result;
    return hash;
  }

  /**
   * Compares the content of the specified buffer to the content of this buffer.  This comparison is
   * performed byte by byte using an unsigned comparison.
   */
  @Override
  public int compareTo(Slice that) {
    if (this == that) {
      return 0;
    }
    if (this.data == that.data && length == that.length && offset == that.offset) {
      return 0;
    }

    int minLength = Math.min(this.length, that.length);
    for (int i = 0; i < minLength; i++) {
      int thisByte = 0xFF & this.data[this.offset + i];
      int thatByte = 0xFF & that.data[that.offset + i];
      if (thisByte != thatByte) {
        return (thisByte) - (thatByte);
      }
    }
    return this.length - that.length;
  }

  /**
   * Decodes this buffer's readable bytes into a string with the specified character set name.
   */
  public String toString(Charset charset) {
    return toString(0, length, charset);
  }

  /**
   * Decodes this buffer's sub-region into a string with the specified character set.
   */
  public String toString(int index, int length, Charset charset) {
    if (length == 0) {
      return "";
    }

    return Slices.decodeString(toByteBuffer(index, length), charset);
  }

  public String toString() {
    return getClass().getSimpleName() + '(' + "length=" + length() + ')';
  }

}