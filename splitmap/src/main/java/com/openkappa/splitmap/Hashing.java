package com.openkappa.splitmap;

public class Hashing {

  public static int jenkins(int value) {
    int hashed = value;
    hashed = (hashed + 0x7ed55d16) + (hashed << 12);
    hashed = (hashed ^ 0xc761c23c) ^ (hashed >> 19);
    hashed = (hashed + 0x165667b1) + (hashed << 5);
    hashed = (hashed + 0xd3a2646c) ^ (hashed << 9);
    hashed = (hashed + 0xfd7046c5) + (hashed << 3);
    hashed = (hashed ^ 0xb55a4f09) ^ (hashed >> 16);
    return hashed;
  }

}
