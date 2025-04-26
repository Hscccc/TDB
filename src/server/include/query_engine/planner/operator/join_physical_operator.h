#pragma once

#include "physical_operator.h"
#include "include/query_engine/structor/tuple/join_tuple.h"

class JoinPhysicalOperator : public PhysicalOperator
{
public:
  JoinPhysicalOperator();
  explicit JoinPhysicalOperator(std::unique_ptr<Expression> condition);
  ~JoinPhysicalOperator() override = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::JOIN;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple *current_tuple() override;

private:
  // 判断当前tuple是否满足join条件
  bool match_condition();

private:
  Trx *trx_ = nullptr;
  JoinedTuple joined_tuple_;  //! 当前关联的左右两个tuple
  std::unique_ptr<Expression> condition_; //! join条件表达式
};
