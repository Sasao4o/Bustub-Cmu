//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)), plan_(plan) {}

void DeleteExecutor::Init() { 
    child_executor_->Init();
    noOfCalls = 0;
 }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    table_oid_t tableId =  this->plan_->TableOid();  
   ExecutorContext *exec_ctx = this->GetExecutorContext();
   Catalog *catalog = exec_ctx->GetCatalog();
   TableInfo*tableInfo = catalog->GetTable(tableId);
    TableHeap *tableHeap = tableInfo->table_.get();
    Tuple childTuple;
    RID childRid;
      std::vector<IndexInfo *> indexInfos = catalog->GetTableIndexes(tableInfo->name_);
      int size = 0;
    while (child_executor_->Next(&childTuple, &childRid)) {
        tableHeap->MarkDelete(childRid, exec_ctx->GetTransaction());
            for (int i = 0; i < indexInfos.size(); i++) {
                Index * index = indexInfos[i]->index_.get();
                index->DeleteEntry(childTuple.KeyFromTuple(tableInfo->schema_, indexInfos[i]->key_schema_, indexInfos[i]->index_->GetKeyAttrs()), *rid, exec_ctx->GetTransaction());
         }
         size++;
    }
         std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount()); 
    values.push_back(Value(INTEGER, size));
    *tuple = Tuple{values, &GetOutputSchema()};
    noOfCalls++;
    if (size == 0 && noOfCalls == 1) return true;
    return size != 0;
}

}  // namespace bustub
 