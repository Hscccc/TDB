#include "include/storage_engine/recover/log_manager.h"
#include "include/storage_engine/transaction/trx.h"
#include "include/storage_engine/transaction/mvcc_trx.h"

RC LogEntryIterator::init(LogFile &log_file)
{
  log_file_ = &log_file;
  return RC::SUCCESS;
}

RC LogEntryIterator::next()
{
  LogEntryHeader header;
  RC rc = log_file_->read(reinterpret_cast<char *>(&header), sizeof(header));
  if (rc != RC::SUCCESS) {
    if (log_file_->eof()) {
      return RC::RECORD_EOF;
    }
    LOG_WARN("failed to read log header. rc=%s", strrc(rc));
    return rc;
  }

  char *data = nullptr;
  int32_t entry_len = header.log_entry_len_;
  if (entry_len > 0) {
    data = new char[entry_len];
    rc = log_file_->read(data, entry_len);
    if (RC_FAIL(rc)) {
      LOG_WARN("failed to read log data. data size=%d, rc=%s", entry_len, strrc(rc));
      delete[] data;
      data = nullptr;
      return rc;
    }
  }

  if (log_entry_ != nullptr) {
    delete log_entry_;
    log_entry_ = nullptr;
  }
  log_entry_ = LogEntry::build(header, data);
  delete[] data;
  return rc;
}

bool LogEntryIterator::valid() const
{
  return log_entry_ != nullptr;
}

const LogEntry &LogEntryIterator::log_entry()
{
  return *log_entry_;
}

////////////////////////////////////////////////////////////////////////////////

LogManager::~LogManager()
{
  if (log_buffer_ != nullptr) {
    delete log_buffer_;
    log_buffer_ = nullptr;
  }

  if (log_file_ != nullptr) {
    delete log_file_;
    log_file_ = nullptr;
  }
}

RC LogManager::init(const char *path)
{
  log_buffer_ = new LogBuffer();
  log_file_   = new LogFile();
  return log_file_->init(path);
}

RC LogManager::append_begin_trx_log(int32_t trx_id)
{
  return append_log(LogEntry::build_mtr_entry(LogEntryType::MTR_BEGIN, trx_id));
}

RC LogManager::append_rollback_trx_log(int32_t trx_id)
{
  return append_log(LogEntry::build_mtr_entry(LogEntryType::MTR_ROLLBACK, trx_id));
}

RC LogManager::append_commit_trx_log(int32_t trx_id, int32_t commit_xid)
{
  RC rc = append_log(LogEntry::build_commit_entry(trx_id, commit_xid));
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to append trx commit log. trx id=%d, rc=%s", trx_id, strrc(rc));
    return rc;
  }
  rc = sync(); // 事务提交时需要把当前事务关联的日志项都写入到磁盘中，这样做是保证不丢数据
  return rc;
}

RC LogManager::append_record_log(LogEntryType type, int32_t trx_id, int32_t table_id, const RID &rid, int32_t data_len, int32_t data_offset, const char *data)
{
  LogEntry *log_entry = LogEntry::build_record_entry(type, trx_id, table_id, rid, data_len, data_offset, data);
  if (nullptr == log_entry) {
    LOG_WARN("failed to create log entry");
    return RC::NOMEM;
  }
  return append_log(log_entry);
}

RC LogManager::append_log(LogEntry *log_entry)
{
  if (nullptr == log_entry) {
    return RC::INVALID_ARGUMENT;
  }
  return log_buffer_->append_log_entry(log_entry);
}

RC LogManager::sync()
{
  return log_buffer_->flush_buffer(*log_file_);
}

// TODO [Lab5] 需要同学们补充代码，相关提示见文档
RC LogManager::recover(Db *db)
{
  TrxManager *trx_manager = GCTX.trx_manager_;
  ASSERT(trx_manager != nullptr, "cannot do recover that trx_manager is null");

  LogEntryIterator log_entry_iter;
  RC rc = log_entry_iter.init(*log_file_);
  if (RC_FAIL(rc)) {
    LOG_ERROR("Failed to initialize log entry iterator. rc=%s", strrc(rc));
    return rc;
  }
  
  std::unordered_map<int32_t, Trx*> active_trxs;
  
  while ((rc = log_entry_iter.next()) == RC::SUCCESS) {
    const LogEntry &log_entry = log_entry_iter.log_entry();
    int32_t trx_id = log_entry.trx_id();
    
    switch (log_entry.log_type()) {
      case LogEntryType::MTR_BEGIN: {
        Trx *trx = dynamic_cast<MvccTrxManager*>(trx_manager)->create_trx(trx_id);
        if (nullptr == trx) {
          LOG_ERROR("Failed to create transaction with id %d during recovery", trx_id);
          return RC::INTERNAL;
        }
        active_trxs[trx_id] = trx;
        break;
      }
      case LogEntryType::MTR_COMMIT:
      case LogEntryType::MTR_ROLLBACK: {
        auto trx_iter = active_trxs.find(trx_id);
        if (trx_iter != active_trxs.end()) {
          MvccTrx *trx = dynamic_cast<MvccTrx*>(trx_iter->second);
          rc = trx->redo(db, log_entry);
          if (RC_FAIL(rc)) {
            LOG_ERROR("Failed to redo %s log for transaction %d. rc=%s",
                     log_entry.log_type() == LogEntryType::MTR_COMMIT ? "COMMIT" : "ROLLBACK",
                     trx_id, strrc(rc));
            return rc;
          }
          active_trxs.erase(trx_iter);
        }
        break;
      }
      case LogEntryType::INSERT:
      case LogEntryType::DELETE: {
        auto trx_iter = active_trxs.find(trx_id);
        if (trx_iter != active_trxs.end()) {
          MvccTrx *trx = dynamic_cast<MvccTrx*>(trx_iter->second);
          rc = trx->redo(db, log_entry);
          if (RC_FAIL(rc)) {
            LOG_ERROR("Failed to redo %s log for transaction %d. rc=%s",
                     log_entry.log_type() == LogEntryType::INSERT ? "INSERT" : "DELETE",
                     trx_id, strrc(rc));
            return rc;
          }
        }
        break;
      }
      default:
        LOG_WARN("Error log entry type: %d", static_cast<int>(log_entry.log_type()));
        break;
    }
  }
  
  if (rc != RC::RECORD_EOF) {
    LOG_ERROR("Error occurred while reading log file. rc=%s", strrc(rc));
    return rc;
  }
  
  for (auto &pair : active_trxs) {
    trx_manager->destroy_trx(pair.second);
  }

  return RC::SUCCESS;
}

