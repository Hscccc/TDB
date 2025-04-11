#include "include/query_engine/planner/operator/index_scan_physical_operator.h"

#include "include/storage_engine/index/index.h"

// TODO [Lab2]
// IndexScanOperator的实现逻辑,通过索引直接获取对应的Page来减少磁盘的扫描

RC IndexScanPhysicalOperator::open(Trx *trx)
{
  if(table_ == nullptr || index_ == nullptr)
  {
    return RC::INTERNAL;
  }

  const char *left_key = left_null_ ? nullptr : left_value_.data();
  const char *right_key = right_null_ ? nullptr : right_value_.data();
  IndexScanner *index_scanner = index_->create_scanner(left_key, 
                                                       left_value_.length(), 
                                                       left_inclusive_,
                                                       right_key, 
                                                       right_value_.length(), 
                                                       right_inclusive_);
  if(index_scanner == nullptr)
  {
    return RC::INTERNAL;
  }

  record_handler_ = table_->record_handler();
  if(record_handler_ == nullptr)
  {
    index_scanner->destroy();
    return RC::INTERNAL;
  }
  index_scanner_ = index_scanner;

  if (table_alias_.empty()) {
    table_alias_ = table_->name();
    LOG_WARN("table alias is empty, use table name as alias.\n"
      "Hint: Consider calling set_table_alias() on IndexScanOperator to set an alias for the table.");
  }

  tuple_.set_schema(table_,table_alias_,table_->table_meta().field_metas());

  return RC::SUCCESS;
}

RC IndexScanPhysicalOperator::next()
{
  RID rid;
  record_page_handler_.cleanup();

  RC rc = index_scanner_->next_entry(&rid, false);
  if (rc == RC::RECORD_EOF) {
    return RC::RECORD_EOF;
  }
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to fetch next entry from index scanner. rc=%s", strrc(rc));
    return rc;
  }

  rc = record_handler_->get_record(record_page_handler_, &rid, readonly_, &current_record_);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to fetch record for RID. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC IndexScanPhysicalOperator::close()
{
  index_scanner_->destroy();
  index_scanner_ = nullptr;
  return RC::SUCCESS;
}

Tuple* IndexScanPhysicalOperator::current_tuple(){
  tuple_._set_record(&current_record_);
  return &tuple_;
}

std::string IndexScanPhysicalOperator::param() const
{
  return std::string(index_->index_meta().name()) + " ON " + table_->name();
}

RC IndexScanPhysicalOperator::filter(RowTuple &tuple, bool &result)
{
  RC rc = RC::SUCCESS;
  Value value;
  for (std::unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(tuple, value);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    bool tmp_result = value.get_boolean();
    if (!tmp_result) {
      result = false;
      return rc;
    }
  }

  result = true;
  return rc;
}