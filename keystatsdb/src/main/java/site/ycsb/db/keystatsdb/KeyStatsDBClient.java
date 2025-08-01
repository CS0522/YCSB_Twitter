package site.ycsb.db.keystatsdb;

import site.ycsb.*;
import site.ycsb.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RocksDB binding for <a href="http://rocksdb.org/">RocksDB</a>.
 *
 * See {@code rocksdb/README.md} for details.
 */
public class KeyStatsDBClient extends DB {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyStatsDBClient.class);

  private static AtomicInteger recordandoperationcounttotal = new AtomicInteger(0);

  @Override
  public void init() throws DBException {
    LOGGER.info("KeyStatsDB is initialized");
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();
    int counttotal = recordandoperationcounttotal.get();
    LOGGER.info("Total Count: " + counttotal);
    LOGGER.info("KeyStatsDB is closed");
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    LOGGER.info("READ: " + key);
    recordandoperationcounttotal.incrementAndGet();
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
    recordandoperationcounttotal.incrementAndGet();
    return Status.OK;
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    LOGGER.info("INSERT: " + key);
    recordandoperationcounttotal.incrementAndGet();
    return Status.OK;
  }

  @Override
  public Status delete(final String table, final String key) {
    return Status.OK;
  }
}
