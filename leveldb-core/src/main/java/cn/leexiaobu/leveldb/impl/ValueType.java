package cn.leexiaobu.leveldb.impl;

/**
 * @author Leexiaobu
 * @date 2023-03-07 14:32
 */
public enum ValueType {
  DELETION(0x00), VALUE(0x01);

  private final int persistentId;

  ValueType(int persistentId) {
    this.persistentId = persistentId;
  }

  public static ValueType getValueTypeByPersistentId(int persistentId) {
    switch (persistentId) {
      case 0:
        return DELETION;
      case 1:
        return VALUE;
      default:
        throw new IllegalArgumentException("Unknown persistentId " + persistentId);
    }
  }

  public int getPersistentId() {
    return persistentId;
  }
}
