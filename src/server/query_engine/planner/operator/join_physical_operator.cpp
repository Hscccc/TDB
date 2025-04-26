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
  
  if (children_[0]->next() != RC::SUCCESS) {
    return RC::RECORD_EOF;  // 左表没有更多记录
  }  
  
  PhysicalOperator *left_oper = children_[0].get();
  PhysicalOperator *right_oper = children_[1].get();
  
  // 检查左表当前是否有记录，如果没有直接返回RECORD_EOF
  if (left_oper->current_tuple() == nullptr) {
    return RC::RECORD_EOF;
  }
  
  RC rc;
  
  // 循环直到找到满足条件的记录或没有更多记录
  while (true) {
    // 尝试获取右子树的下一条记录
    rc = right_oper->next();
    
    // 如果右子树已经到达末尾，需要获取左子树的下一条记录，并重置右子树
    if (rc != RC::SUCCESS) {
      right_oper->close();
      
      // 获取左子树的下一条记录
      rc = left_oper->next();
      if (rc != RC::SUCCESS) {
        // 左子树也已经到达末尾，整个连接操作完成
        return RC::RECORD_EOF;
      }
      
      // 重新打开右子树
      rc = right_oper->open(trx_);
      if (rc != RC::SUCCESS) {
        return rc;
      }
      
      // 获取右子树的第一条记录
      rc = right_oper->next();
      if (rc != RC::SUCCESS) {
        // 右子树为空，继续下一个左子树记录
        // 由于右表为空，内连接结果也为空，继续循环
        continue;
      }
    }
    
    // 获取左右子树的当前元组
    Tuple *left_tuple = left_oper->current_tuple();
    Tuple *right_tuple = right_oper->current_tuple();
    
    // 确保两个tuple都有效
    if (left_tuple == nullptr || right_tuple == nullptr) {
      continue;
    }
    
    // 设置连接元组
    joined_tuple_.set_left(left_tuple);
    joined_tuple_.set_right(right_tuple);
    
    // 检查是否满足连接条件
    if (match_condition()) {
      // 找到满足条件的记录，返回成功
      return RC::SUCCESS;
    }
    
  }
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
