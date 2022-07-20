package com.helios.spark;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class CSVLineRecordReaderTest {
  private final CSVLineRecordReader recordReader = new CSVLineRecordReader();
  private Method methodRemoveAndAnonymizeColumn;
  private Method methodAnonymize;
  private Method methodSplitByteArrayByDelimiter;
  private Method methodMerge2DArray;
  private final Set<Integer> indicesToAnonymize =
      new HashSet<Integer>() {
        {
          add(3);
        }
      };
  private final Set<Integer> indicesToRemove =
      new HashSet<Integer>() {
        {
          add(2);
          add(4);
        }
      };
  private final CSVColumnIndicesInfo csvColumnIndicesInfo =
      new CSVColumnIndicesInfo(indicesToAnonymize, indicesToRemove);
  private final byte fieldSep = (byte) ',';

  @Before
  public void setUp() {
    initPrivateMethods();
    initPrivateFields();
  }

  private void initPrivateMethods() {
    try {
      methodRemoveAndAnonymizeColumn =
          CSVLineRecordReader.class.getDeclaredMethod("removeAndAnonymizeColumn", Text.class);
      methodRemoveAndAnonymizeColumn.setAccessible(true);

      methodAnonymize = CSVLineRecordReader.class.getDeclaredMethod("anonymize", byte[].class);
      methodAnonymize.setAccessible(true);

      methodMerge2DArray =
          CSVLineRecordReader.class.getDeclaredMethod("merge2DArray", List.class, byte.class);
      methodMerge2DArray.setAccessible(true);

      methodSplitByteArrayByDelimiter =
          CSVLineRecordReader.class.getDeclaredMethod(
              "splitByteArrayByDelimiter", byte[].class, byte.class);
      methodSplitByteArrayByDelimiter.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void initPrivateFields() {
    try {
      Field fieldColumnIndicesInfo =
          CSVLineRecordReader.class.getDeclaredField("columnIndicesInfo");
      fieldColumnIndicesInfo.setAccessible(true);
      fieldColumnIndicesInfo.set(recordReader, csvColumnIndicesInfo);

      Field fieldFieldSep = CSVLineRecordReader.class.getDeclaredField("fieldSep");
      fieldFieldSep.setAccessible(true);
      fieldFieldSep.setByte(recordReader, fieldSep);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testRemoveAndAnonymizeColumn()
      throws InvocationTargetException, IllegalAccessException {
    Text text = new Text();
    text.set("f1,f2,f3_should_be_removed,f4_should_be_anonymized,f5_should_be_removed");

    Text expected = new Text();
    String f4_anonymized =
        new String(
            (byte[]) methodAnonymize.invoke(recordReader, "f4_should_be_anonymized".getBytes()));
    expected.set(String.format("f1,f2,%s", f4_anonymized));

    Text actual = (Text) methodRemoveAndAnonymizeColumn.invoke(recordReader, text);

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testMerge2DArray() throws InvocationTargetException, IllegalAccessException {
    List<byte[]> arrayList =
        new ArrayList<byte[]>() {
          {
            add(new byte[] {1, 2});
            add(new byte[] {3, 4});
            add(new byte[] {5, 6});
          }
        };
    byte delimiter = 0;

    byte[] expected = new byte[] {1, 2, 0, 3, 4, 0, 5, 6};

    byte[] actual = (byte[]) methodMerge2DArray.invoke(recordReader, arrayList, delimiter);

    assertArrayEquals(expected, actual);
  }

  @Test
  public void testSplitByteArrayByDelimiter()
      throws InvocationTargetException, IllegalAccessException {
    byte[] array = new byte[] {1, 2, 0, 3, 4, 0, 5, 6};
    byte delimiter = 0;

    List<byte[]> expected =
        new ArrayList<byte[]>() {
          {
            add(new byte[] {1, 2});
            add(new byte[] {3, 4});
            add(new byte[] {5, 6});
          }
        };

    List<byte[]> actual =
        (List<byte[]>) methodSplitByteArrayByDelimiter.invoke(recordReader, array, delimiter);

    assertEqualListOfBytes(expected, actual);
  }

  private void assertEqualListOfBytes(List<byte[]> list_1, List<byte[]> list_2) {
    for (int i = 0; i < list_1.size(); i++) {
      byte[] element_1 = list_1.get(i);
      byte[] element_2 = list_2.get(i);
      assertArrayEquals(element_1, element_2);
    }
  }
}
