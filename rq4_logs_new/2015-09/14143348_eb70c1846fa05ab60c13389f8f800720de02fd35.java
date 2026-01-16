package com.github.fakemongo;

import com.github.fakemongo.junit.FongoRule;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class FongoGridFSTest {

  @Rule
  public final FongoRule fongoRule = new FongoRule(false);

  private GridFS fs;

  @Before
  public void setUp() throws Exception {
    fs = new GridFS(fongoRule.getDB());
  }

  @Test
  public void shouldStoreFileInMultipleChunks() throws Exception {
    final byte[] data = new byte[]{1, 2, 3, 4, 5};

    final GridFSInputFile file = fs.createFile(data);
    file.setChunkSize(3); //chunk size is less than data size in order to get more than one chunk
    file.save();

    final GridFSDBFile result = fs.findOne((ObjectId) file.getId());

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    assertEquals(data.length, result.writeTo(out));

    assertArrayEquals(data, out.toByteArray());
  }

}