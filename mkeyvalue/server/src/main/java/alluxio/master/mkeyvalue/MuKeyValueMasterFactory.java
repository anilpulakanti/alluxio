/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.mkeyvalue;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.master.Master;
import alluxio.master.MasterFactory;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.ReadWriteJournal;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link KeyValueMaster} instance.
 */
@ThreadSafe
public final class MuKeyValueMasterFactory implements MasterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Constructs a new {@link MuKeyValueMasterFactory}.
   */
  public MuKeyValueMasterFactory() {}

  @Override
  public boolean isEnabled() {
    return Configuration.getBoolean(PropertyKey.KEY_VALUE_ENABLED);
  }

  @Override
  public String getName() {
    return Constants.MU_KEY_VALUE_MASTER_NAME;
  }

  @Override
  public MuKeyValueMaster create(List<? extends Master> masters, String journalDirectory) {
    if (!isEnabled()) {
      return null;
    }
    Preconditions.checkArgument(journalDirectory != null, "journal path may not be null");
    LOG.info("Creating {} ", MuKeyValueMaster.class.getName());

    ReadWriteJournal journal =
        new ReadWriteJournal(MuKeyValueMaster.getJournalDirectory(journalDirectory));

    for (Master master : masters) {
      if (master instanceof FileSystemMaster) {
        LOG.info("{} is created", MuKeyValueMaster.class.getName());
        return new MuKeyValueMaster((FileSystemMaster) master, journal);
      }
    }
    LOG.error("Fail to create {} due to missing {}", MuKeyValueMaster.class.getName(),
        FileSystemMaster.class.getName());
    return null;
  }

}
