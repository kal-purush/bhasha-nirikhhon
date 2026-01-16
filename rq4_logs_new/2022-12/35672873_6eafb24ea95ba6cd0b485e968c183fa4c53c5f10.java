/*
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
package org.gbif.metadata.common.util;

import org.gbif.api.vocabulary.MetadataType;
import org.gbif.utils.file.FileUtils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MetadataUtilsTest {

  @Test
  public void testDetectParserType() throws Exception {
    MetadataType type =
        MetadataUtils.detectParserType(FileUtils.classpathStream("dc/worms_dc.xml"));
    assertEquals(MetadataType.DC, type);

    type = MetadataUtils.detectParserType(FileUtils.classpathStream("eml/ipt_eml.xml"));
    assertEquals(MetadataType.EML, type);

    type = MetadataUtils.detectParserType(FileUtils.classpathStream("eml/clb_eml.xml"));
    assertEquals(MetadataType.EML, type);

    type =
        MetadataUtils.detectParserType(
            FileUtils.classpathStream("eml-metadata-profile/sample2-v1.0.1.xml"));
    assertEquals(MetadataType.EML, type);

    type =
        MetadataUtils.detectParserType(
            FileUtils.classpathStream("eml-metadata-profile/sample4-v1.1.xml"));
    assertEquals(MetadataType.EML, type);

    type = MetadataUtils.detectParserType(FileUtils.classpathStream("eml/eml_utf8_bom.xml"));
    assertEquals(MetadataType.EML, type);

    type = MetadataUtils.detectParserType(FileUtils.classpathStream("eml/sample-breaking.xml"));
    assertEquals(MetadataType.EML, type);

    assertIllegalArg("eml/eml-protocol.xml");
    assertIllegalArg("logback-test.xml");
    assertIllegalArg("dc/dc_broken.xml");
  }

  private void assertIllegalArg(String classpathFile) {
    assertThrows(
        IllegalArgumentException.class,
        () -> MetadataUtils.detectParserType(FileUtils.classpathStream(classpathFile)));
  }
}