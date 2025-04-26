#include "include/query_engine/planner/operator/join_physical_operator.h"

JoinPhysicalOperator::JoinPhysicalOperator() = default;

JoinPhysicalOperator::JoinPhysicalOperator(std::unique_ptr<Expression> condition)
  : condition_(std::move(condition))
{
}

RC JoinPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 2) {
    LOG_WARN("JoinPhysicalOperator requires exactly two children");
    return RC::INTERNAL;
  }

  trx_ = trx;
  
  // 打开左子树
  RC rc = children_[0]->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to open left child of join operator");
    return rc;
  }
  
  // 打开右子树
  rc = children_[1]->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to open right child of join operator");
    children_[0]->close();
    return rc;
  }

  rc = children_[0]->next();
  if (rc != RC::SUCCESS) {
    left_is_empty = true;
  }
  
  return RC::SUCCESS;
}

// 判断当前tuple是否满足join条件
bool JoinPhysicalOperator::match_condition()
{
  if (!condition_) {
    return true;
  }
  
  Value value;
  RC rc = condition_->get_value(joined_tuple_, value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to evaluate join condition");
    return false;
  }
  
  return value.get_boolean();
}

RC JoinPhysicalOperator::next()
{
  if (children_.size() != 2) {
    return RC::INTERNAL;
  }

  if (left_is_empty) {
    return RC::RECORD_EOF;
  }
  
  while (true) {
    Tuple *left_tuple = children_[0]->current_tuple();
    
    while (children_[1]->next() == RC::SUCCESS) {
      Tuple *right_tuple = children_[1]->current_tuple();
      
      // 进行连接
      joined_tuple_.set_left(left_tuple);
      joined_tuple_.set_right(right_tuple);
      
      // 检查连接条件
      if (match_condition()) {
        return RC::SUCCESS;
      }
    }
    
    // 重置右子树
    children_[1]->close();
    children_[1]->open(trx_);

    if (children_[0]->next() != RC::SUCCESS) {
      // 左子树已经遍历完毕
      children_[0]->close();
      children_[1]->close();
      return RC::RECORD_EOF;
    }
  }

  return RC::RECORD_EOF;
}

RC JoinPhysicalOperator::close()
{
  RC rc = RC::SUCCESS;
  
  // 关闭所有子算子
  for (auto &child : children_) {
    RC child_rc = child->close();
    if (child_rc != RC::SUCCESS) {
      LOG_WARN("Failed to close child of join operator");
      rc = child_rc;
    }
  }
  
  return rc;
}

Tuple *JoinPhysicalOperator::current_tuple()
{
  return &joined_tuple_;
}
