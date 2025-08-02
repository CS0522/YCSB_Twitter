package site.ycsb.db.keystatsdb;

import site.ycsb.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * KeyStatsDB binding for Key statistics only.
 *
 * See {@code keystatsdb/README.md} for details.
 */
public class KeyStatsDBClient extends DB {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyStatsDBClient.class);

  private final ConcurrentMap<String, LongAdder> keyStats = new ConcurrentHashMap<>();

  @Override
  public void init() throws DBException {
    LOGGER.info("KeyStatsDB is initialized");
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();
    outputStats();
    LOGGER.info("KeyStatsDB is closed");
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    LOGGER.info("READ: " + key);
    recordOperation(key);
    return Status.OK; 
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
      final Vector<HashMap<String, ByteIterator>> result) {
    LOGGER.error("Cannot support scan");
    return Status.OK;
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    LOGGER.info("UPDATE: " + key);
    recordOperation(key);
    return Status.OK;
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    LOGGER.info("INSERT: " + key);
    recordOperation(key);
    return Status.OK;
  }

  @Override
  public Status delete(final String table, final String key) {
    return Status.OK;
  }

  private void recordOperation(String key) {
    keyStats.computeIfAbsent(key, k -> new LongAdder()).increment();
  }

  private void writeToFile(String filename, Collection<?> data) throws IOException {
    try (PrintWriter pw = new PrintWriter(filename)) {
      for (Object entry : data) {
        if (entry instanceof Map.Entry) {
          Map.Entry<?, LongAdder> e = (Map.Entry<?, LongAdder>) entry;
          pw.printf("%s,%d%n", e.getKey(), e.getValue().sum());
        }
      }
    }
  }

  private void outputStats() {
    try {
      // 1. 原始键统计
      writeToFile("key_stats.csv", keyStats.entrySet());
      // 2. 频率降序
      List<Map.Entry<String, LongAdder>> frequencyOrder = new ArrayList<>(keyStats.entrySet());
      frequencyOrder.sort((e1, e2) -> 
          Long.compare(e2.getValue().sum(), e1.getValue().sum()));
      writeToFile("key_stats_descend.csv", frequencyOrder);
      // 3. 字典序
      List<Map.Entry<String, LongAdder>> dictOrder = new ArrayList<>(keyStats.entrySet());
      dictOrder.sort((e1, e2) -> {
          int lenCmp = Integer.compare(e1.getKey().length(), e2.getKey().length());
          return lenCmp != 0 ? lenCmp : e1.getKey().compareTo(e2.getKey());
        });
      writeToFile("key_stats_dict_ordered.csv", dictOrder);
    } catch (IOException e) {
      LOGGER.error("File export failed: {}", e.getMessage());
    }
  }
}
