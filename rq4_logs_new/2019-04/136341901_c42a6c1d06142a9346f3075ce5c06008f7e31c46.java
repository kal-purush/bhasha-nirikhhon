/*
 * Copyright (c) 2012-2016 Sonatype, Inc. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except content compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to content writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.ning.http.client.providers.grizzly;

import static org.glassfish.grizzly.memory.MemoryManager.DEFAULT_MEMORY_MANAGER;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;

import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.memory.Buffers;
import org.slf4j.Logger;

public class NonBlockingInputStreamFeeder extends FeedableBodyGenerator.NonBlockingFeeder {

  private static final Logger LOGGER = getLogger(NonBlockingInputStreamFeeder.class);

  private static final int DEFAULT_INTERNAL_BUFFER_SIZE = 8192;

  private final InputStream content;
  private byte[] bytesIn;
  private boolean isDone;

  public NonBlockingInputStreamFeeder(FeedableBodyGenerator feedableBodyGenerator, InputStream content) {
    this(feedableBodyGenerator, content, DEFAULT_INTERNAL_BUFFER_SIZE);
  }

  public NonBlockingInputStreamFeeder(FeedableBodyGenerator feedableBodyGenerator, InputStream content, int internalBufferSize) {
    super(feedableBodyGenerator);
    this.bytesIn = new byte[internalBufferSize];
    this.content = content;
  }

  @Override
  public void canFeed() throws IOException {
    final int read = content.read(bytesIn);
    if (read == -1) {
      isDone = true;
      feed(Buffers.EMPTY_BUFFER, true);
      return;
    }

    if (read == 0) {
      feed(Buffers.EMPTY_BUFFER, false);
      return;
    }

    final Buffer b = Buffers.wrap(DEFAULT_MEMORY_MANAGER, bytesIn, 0, read);
    feed(b, false);
  }

  @Override
  public boolean isDone() {
    return isDone;
  }

  @Override
  public boolean isReady() {
    // only reason to not be ready is being done
    return !isDone;
  }

  @Override
  public void notifyReadyToFeed(final FeedableBodyGenerator.NonBlockingFeeder.ReadyToFeedListener listener) {
    if (isReady()) {
      listener.ready();
    }
  }

  @Override
  public void reset() {
    // make sure state and our IS are reset
    isDone = false;
    if (content.markSupported()) {
      try {
        content.reset();
      } catch (IOException ioe) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Unable to reset the input stream: {}", ioe.getMessage());
        }
      }

      content.mark(0);
    }
    super.reset();
  }
}