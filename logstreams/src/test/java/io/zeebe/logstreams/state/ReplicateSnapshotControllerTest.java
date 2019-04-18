/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
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
package io.zeebe.logstreams.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import io.zeebe.db.impl.DefaultColumnFamily;
import io.zeebe.db.impl.rocksdb.ZeebeRocksDbFactory;
import io.zeebe.logstreams.processor.SnapshotChunk;
import io.zeebe.logstreams.processor.SnapshotReplication;
import io.zeebe.logstreams.util.RocksDBWrapper;
import io.zeebe.test.util.AutoCloseableRule;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ReplicateSnapshotControllerTest {
  @Rule public TemporaryFolder tempFolderRule = new TemporaryFolder();
  @Rule public AutoCloseableRule autoCloseableRule = new AutoCloseableRule();

  private StateSnapshotController replicatorSnapshotController;
  private StateSnapshotController receiverSnapshotController;
  private Replicator replicator;

  @Before
  public void setup() throws IOException {
    final File runtimeDirectory = tempFolderRule.newFolder("runtime");
    final File snapshotsDirectory = tempFolderRule.newFolder("snapshots");
    final DataStorage storage = new DataStorage(runtimeDirectory, snapshotsDirectory);

    final File receiverRuntimeDirectory = tempFolderRule.newFolder("runtime-receiver");
    final File receiverSnapshotsDirectory = tempFolderRule.newFolder("snapshots-receiver");
    final DataStorage receiverStorage =
        new DataStorage(receiverRuntimeDirectory, receiverSnapshotsDirectory);

    replicator = new Replicator();
    replicatorSnapshotController =
        new StateSnapshotController(
            storage, replicator, ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class));
    receiverSnapshotController =
        new StateSnapshotController(
            receiverStorage, replicator, ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class));

    autoCloseableRule.manage(replicatorSnapshotController);
    autoCloseableRule.manage(receiverSnapshotController);

    final String key = "test";
    final int value = 0xCAFE;
    final RocksDBWrapper wrapper = new RocksDBWrapper();
    wrapper.wrap(replicatorSnapshotController.openDb());
    wrapper.putInt(key, value);
  }

  @Test
  public void shouldReplicateSnapshotChunks() {
    // given
    replicatorSnapshotController.takeSnapshot(1);

    // when
    replicatorSnapshotController.replicateLatestSnapshot();

    // then
    final List<SnapshotChunk> replicatedChunks = replicator.replicatedChunks;
    final int totalCount = replicatedChunks.size();
    assertThat(totalCount).isGreaterThan(0);

    final SnapshotChunk firstChunk = replicatedChunks.get(0);
    final int chunkTotalCount = firstChunk.getTotalCount();
    assertThat(totalCount).isEqualTo(chunkTotalCount);

    assertThat(replicatedChunks)
        .extracting(SnapshotChunk::getSnapshotPosition, SnapshotChunk::getTotalCount)
        .containsOnly(tuple(1L, totalCount));
  }

  @Test
  public void shouldReceiveSnapshotChunks() throws Exception {
    // given
    receiverSnapshotController.consumeReplicatedSnapshots();
    replicatorSnapshotController.takeSnapshot(1);

    // when
    replicatorSnapshotController.replicateLatestSnapshot();

    // then
    final String key = "test";
    final RocksDBWrapper wrapper = new RocksDBWrapper();
    final long recoveredSnapshot = receiverSnapshotController.recover();
    assertThat(recoveredSnapshot).isEqualTo(1);

    wrapper.wrap(receiverSnapshotController.openDb());
    final int valueFromSnapshot = wrapper.getInt(key);
    assertThat(valueFromSnapshot).isEqualTo(0xCAFE);
  }

  private final class Replicator implements SnapshotReplication {

    final List<SnapshotChunk> replicatedChunks = new ArrayList<>();
    private Consumer<SnapshotChunk> chunkConsumer;

    @Override
    public void replicate(SnapshotChunk snapshot) {
      replicatedChunks.add(snapshot);
      if (chunkConsumer != null) {
        chunkConsumer.accept(snapshot);
      }
    }

    @Override
    public void consume(Consumer<SnapshotChunk> consumer) {
      chunkConsumer = consumer;
    }

    @Override
    public void close() {}
  }
}