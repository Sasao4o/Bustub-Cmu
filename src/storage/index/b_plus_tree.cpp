#include <string>

#include "common/exception.h"

#include "common/logger.h"

#include "common/rid.h"

#include "storage/index/b_plus_tree.h"

#include "storage/page/header_page.h"

namespace bustub {
  INDEX_TEMPLATE_ARGUMENTS
  BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager * buffer_pool_manager,
      const KeyComparator & comparator,
        int leaf_max_size, int internal_max_size): index_name_(std::move(name)),
    root_page_id_(INVALID_PAGE_ID),
    buffer_pool_manager_(buffer_pool_manager),
    comparator_(comparator),
    leaf_max_size_(leaf_max_size),
    internal_max_size_(internal_max_size) {}

  /*
   * Helper function to decide whether current b+tree is empty
   */
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
    return root_page_id_ == INVALID_PAGE_ID;
  }
  /*****************************************************************************
   * SEARCH
   *****************************************************************************/
  /*
   * Return the only value that associated with input key
   * This method is used for point query
   * @return : true means key exists
   */
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::GetValue(const KeyType & key, std::vector < ValueType > * result, Transaction * transaction) -> bool {
    LeafPage * leafPage = FindLeaf(key, root_page_id_);
    if (leafPage == 0) return false;
    return leafPage -> GetValue(key, comparator_, result);

  }

  /*****************************************************************************
   * INSERTION
   *****************************************************************************/
  /*
   * Insert constant key & value pair into b+ tree
   * if current tree is empty, start new tree, update root page id and insert
   * entry, otherwise insert into leaf page.
   * @return: since we only support unique key, if user try to insert duplicate
   * keys return false, otherwise return true.
   */
  INDEX_TEMPLATE_ARGUMENTS
  template < typename T >
    auto BPLUSTREE_TYPE::BuildRootNode(int maxSize) -> T * {
      page_id_t currentPageId;
      Page * rawPage = buffer_pool_manager_ -> NewPage( & currentPageId);
      if (rawPage == nullptr) return 0;
      T * rootPage = reinterpret_cast < T * > (rawPage -> GetData());
      root_page_id_ = currentPageId;
      rootPage -> Init(currentPageId, INVALID_PAGE_ID, maxSize);
      UpdateRootPageId(0);
      return rootPage;
    }

  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::Insert(const KeyType & key,
    const ValueType & value, Transaction * transaction) -> bool {
    if (root_page_id_ == INVALID_PAGE_ID) {
      //1st Insertion
      LeafPage * rootPage = BuildRootNode < LeafPage > (leaf_max_size_);
      bool result = rootPage -> Insert(key, value, comparator_);
      buffer_pool_manager_ -> UnpinPage(root_page_id_, true);
      return result;
    }

    LeafPage * ourLeaf = FindLeaf(key, root_page_id_);
    if (ourLeaf -> KeyExist(key, comparator_)) {
      buffer_pool_manager_ -> UnpinPage(ourLeaf -> GetPageId(), false);
      return false;
    }
    if (ourLeaf -> IsFull()) {
      //We Need to Split
      LeafPage * returnedLeaf;
      MappingType newPair = std::make_pair(key, value);
       SplitLeafNode(ourLeaf, & returnedLeaf, newPair);

      page_id_t parentId = returnedLeaf -> GetParentPageId();
      InternalPage * parentPage;
      if (parentId == INVALID_PAGE_ID) {
        //Here i need to create new root 
         parentPage = BuildRootNode < InternalPage > (internal_max_size_);
        parentId = parentPage -> GetPageId();
        ourLeaf->SetParentPageId(parentId);
        returnedLeaf->SetParentPageId(parentId);
      } else { 
      Page * rawParentPage = buffer_pool_manager_ -> FetchPage(parentId);
       if (rawParentPage == nullptr) return false;
        parentPage = reinterpret_cast < InternalPage * > (rawParentPage -> GetData());
      }
      
       
         std::pair < KeyType, page_id_t > m = std::make_pair(returnedLeaf -> KeyAt(0), returnedLeaf -> GetPageId());
         InsertIntoParent(parentPage, m.first,returnedLeaf, ourLeaf -> GetPageId(), transaction);
         
      // parentPage->Insert(ourLeaf->GetPageId(), returnedLeaf->KeyAt(0), returnedLeaf->GetPageId(), comparator_);
      buffer_pool_manager_ -> UnpinPage(parentId, true);
      buffer_pool_manager_ -> UnpinPage(ourLeaf -> GetPageId(), true);
        buffer_pool_manager_ -> UnpinPage(returnedLeaf -> GetPageId(), true);
        // BPlusTreePage* bpTree = reinterpret_cast < BPlusTreePage * > (buffer_pool_manager_ -> FetchPage(root_page_id_) -> GetData());
        //  LOG_DEBUG("Root page id is %d", bpTree->GetPageId());
        //  LOG_DEBUG("Size is %d" ,bpTree->GetSize());
      return true;
    } else {
      bool result = ourLeaf -> Insert(key, value, comparator_);
      buffer_pool_manager_ -> UnpinPage(ourLeaf -> GetPageId(), result);
      return result;
    }
  }

  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::FindLeaf(KeyType key, page_id_t pageId) -> LeafPage * {
    Page * rawPage = buffer_pool_manager_ -> FetchPage(pageId);
    if (rawPage == nullptr) return 0;

    BPlusTreePage * currentPage = reinterpret_cast < BPlusTreePage * > (rawPage -> GetData());
    if (currentPage -> IsLeafPage()) {
      LeafPage * myPage = reinterpret_cast < LeafPage * > (rawPage -> GetData());
      return myPage;
    } else {
      //This is internal node
      InternalPage * myPage = reinterpret_cast < InternalPage * > (rawPage -> GetData());
      bool found = false;
      page_id_t childPosition = -1;
      for (int i = 1; i < myPage -> GetArraySize(); i++) {
        if (comparator_(key, myPage -> KeyAt(i)) < 0) {
          found = true;
          childPosition = myPage -> ValueAt(i - 1);
          break;
        }
      }
      if (found == false) {
        childPosition = myPage -> ValueAt(myPage -> GetArraySize() - 1);
      }
      buffer_pool_manager_ -> UnpinPage(pageId, false);
      return FindLeaf(key, childPosition);
    }

  }

  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::SplitLeafNode(LeafPage * oldLeafPage, LeafPage ** returnedNewPage, std::pair < KeyType, ValueType > & newPair) -> void {
    std::vector < std::pair < KeyType, ValueType >> temporaryLeafPage;
    bool done = false;
    int arrayLength = oldLeafPage->GetSize();
    for (int i = 0; i < arrayLength; i++) {
      MappingType m = oldLeafPage -> pop();
      if (!done && comparator_(newPair.first, m.first) > 0) {
        done = true;
        temporaryLeafPage.push_back(newPair);
      }
      temporaryLeafPage.push_back(m);
    }
    if (!done) {
      temporaryLeafPage.push_back(newPair);
      done = true;
    }
    page_id_t newPageId;
    Page * newPage = buffer_pool_manager_ -> NewPage( & newPageId);
    if (newPage == nullptr) return;
    LeafPage * newLeafPage = reinterpret_cast < LeafPage * > (newPage -> GetData());
    newLeafPage -> Init(newPageId, oldLeafPage -> GetParentPageId(), oldLeafPage -> GetMaxSize());
    newLeafPage -> SetNextPageId(oldLeafPage -> GetNextPageId());
    oldLeafPage -> SetNextPageId(newPageId);
    int median = ceil((temporaryLeafPage.size() / 2.0));
    for (int i = 0; i < median; i++) {
      MappingType keyValuePair = temporaryLeafPage.back();
      temporaryLeafPage.pop_back();
      oldLeafPage -> Insert(keyValuePair.first, keyValuePair.second, comparator_);
    }
    int temporaryArraySize = temporaryLeafPage.size();
    for (int i = 0; i < temporaryArraySize; i++) {
      MappingType keyValuePair = temporaryLeafPage.back();
      temporaryLeafPage.pop_back();
      newLeafPage -> Insert(keyValuePair.first, keyValuePair.second, comparator_);
    }
    *returnedNewPage = newLeafPage;
 
    return;
  }

  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::InsertInFullInternal(const KeyType &k,const page_id_t &Pointer,InternalPage * oldInternalPage, InternalPage ** returnedNewPage) -> std::pair<KeyType, page_id_t> {
        int arrayLength = oldInternalPage->GetArraySize();
        if (arrayLength != oldInternalPage->GetMaxSize() + 1) return GetInvalidPair();
    page_id_t newPageId;
    Page * newPage = buffer_pool_manager_ -> NewPage( & newPageId);
    InternalPage * newInternalPage = reinterpret_cast < InternalPage * > (newPage -> GetData());
    newInternalPage -> Init(newPageId, oldInternalPage -> GetParentPageId(), oldInternalPage -> GetMaxSize());
    BPlusTreeInternalPage < KeyType, page_id_t  , KeyComparator > temporaryPage;
    temporaryPage.Init(INVALID_PAGE_ID, INVALID_PAGE_ID, oldInternalPage->GetMaxSize() + 1);
    // temporaryPage.SetMaxSize(oldInternalPage -> GetMaxSize() + 1); //Just For Holding all so i can split easily (This step is for simplification)
    for (int i = 1; i < arrayLength; i++) {
 
      page_id_t leftPointer = oldInternalPage -> ValueAt(i - 1);
      page_id_t rightPointer = oldInternalPage -> ValueAt(i);
      KeyType key = oldInternalPage -> KeyAt(i);
     LOG_DEBUG("I am insterting to temprary the left value %d", leftPointer);
      temporaryPage.Insert(leftPointer, key, rightPointer, comparator_  );
    }
    temporaryPage.InsertAndShift(k, Pointer, comparator_);
  
         while (oldInternalPage->GetSize() > 0) {
      oldInternalPage->pop();
     }
    int median = floor(temporaryPage.GetSize() / 2.0);

    for (int i = 1; i < median + 1; i++) {
        LOG_DEBUG("Im inserting to the left splitted internal to left %d", temporaryPage.ValueAt(i - 1));
        LOG_DEBUG("Im inserting to the left splitted internal to right %d", temporaryPage.ValueAt(i));
      oldInternalPage -> Insert(temporaryPage.ValueAt(i - 1), temporaryPage.KeyAt(i), temporaryPage.ValueAt(i), comparator_);
    }
    for (int i = median + 2; i < temporaryPage.GetSize() + 1; i++) {
       LOG_DEBUG("Im inserting to the right splitted internal to the left %d", temporaryPage.ValueAt(i - 1));
           LOG_DEBUG("Im inserting to the right splitted internal to the right %d", temporaryPage.ValueAt(i));
      newInternalPage -> Insert(temporaryPage.ValueAt(i - 1), temporaryPage.KeyAt(i), temporaryPage.ValueAt(i), comparator_);
    }
    for (int i = 0; i < newInternalPage->GetArraySize(); i++) {
      //This Loop is responsible for updating parent pointers
      page_id_t currentPageId = newInternalPage->ValueAt(i);
      Page * rawPage = buffer_pool_manager_ -> FetchPage(currentPageId);
      if (rawPage == nullptr) return GetInvalidPair();
      BPlusTreePage * BPage = reinterpret_cast < BPlusTreePage * > (rawPage -> GetData());
      BPage->SetParentPageId(newInternalPage->GetPageId());
      buffer_pool_manager_->UnpinPage(newInternalPage->GetPageId(), true);
    }
    //Median + 1 is sent to the caller so he insert it to the parent 
    std::pair<KeyType, page_id_t> returnPair = std::make_pair(temporaryPage.KeyAt(median + 1), newInternalPage -> GetPageId());
    *returnedNewPage = newInternalPage;
    return returnPair;
  }

  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::InsertIntoParent(InternalPage * currentInternal, KeyType & key, BPlusTreePage* currentPage , page_id_t brotherId, Transaction * transaction) -> void {
        if (currentInternal->IsFull()) {
          InternalPage *brotherPage;
          InternalPage *parentPage;
          page_id_t parentPageId = currentInternal->GetParentPageId(); 
          std::pair<KeyType,page_id_t> returnedPair = InsertInFullInternal(key, currentPage->GetPageId(), currentInternal, &brotherPage);
          if (parentPageId == INVALID_PAGE_ID) {
        //Here i need to create new rgit oot 
         parentPage = BuildRootNode < InternalPage > (internal_max_size_);
         if (parentPage == 0) return;
          parentPageId = parentPage -> GetPageId();
      } else { 
        Page * rawParentPage = buffer_pool_manager_ -> FetchPage(parentPageId);
         if (rawParentPage == nullptr) return ;
         parentPage = reinterpret_cast < InternalPage * > (rawParentPage -> GetData());
        }
         currentInternal->SetParentPageId(parentPageId);
        brotherPage->SetParentPageId(parentPageId);
        currentPage->SetParentPageId(brotherPage->GetPageId());
        // currentPage->SetParentPageId(returnedPair.second);
           LOG_DEBUG("AFTER EVERY THING THE NEWINTERNALPAGE SIZE IS %d", brotherPage->ValueAt(0));
           InsertIntoParent(parentPage,returnedPair.first, brotherPage, currentInternal->GetPageId(), transaction);
        //   parentPage->Insert(currentInternal->GetPageId(), returnedPair.first, brotherPage->GetPageId(), comparator_);
          
           buffer_pool_manager_ -> UnpinPage(brotherPage->GetPageId(), true);
           buffer_pool_manager_ -> UnpinPage(parentPageId, true);
         } 
         else {
          currentInternal->Insert(brotherId, key, currentPage->GetPageId(), comparator_);
        }
  }

  /*****************************************************************************
   * REMOVE
   *****************************************************************************/
  /*
   * Delete key & value pair associated with input key
   * If current tree is empty, return immdiately.
   * If not, User needs to first find the right leaf page as deletion target, then
   * delete entry from leaf page. Remember to deal with redistribute or merge if
   * necessary.
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::Remove(const KeyType & key, Transaction * transaction) {}

  /*****************************************************************************
   * INDEX ITERATOR
   *****************************************************************************/
  /*
   * Input parameter is void, find the leaftmost leaf page first, then construct
   * index iterator
   * @return : index iterator
   */
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
    return INDEXITERATOR_TYPE();
  }

  /*
   * Input parameter is low key, find the leaf page that contains the input key
   * first, then construct index iterator
   * @return : index iterator
   */
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::Begin(const KeyType & key) -> INDEXITERATOR_TYPE {
    return INDEXITERATOR_TYPE();
  }

  /*
   * Input parameter is void, construct an index iterator representing the end
   * of the key/value pair in the leaf node
   * @return : index iterator
   */
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
    return INDEXITERATOR_TYPE();
  }

  /**
   * @return Page id of the root of this tree
   */
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
    return root_page_id_;
  }

  /*****************************************************************************
   * UTILITIES AND DEBUG
   *****************************************************************************/
  /*
   * Update/Insert root page id in header page(where page_id = 0, header_page is
   * defined under include/page/header_page.h)
   * Call this method everytime root page id is changed.
   * @parameter: insert_record      defualt value is false. When set to true,
   * insert a record <index_name, root_page_id> into header page instead of
   * updating it.
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
    auto * header_page = static_cast < HeaderPage * > (buffer_pool_manager_ -> FetchPage(HEADER_PAGE_ID));
    if (insert_record != 0) {
      // create a new record<index_name + root_page_id> in header_page
      header_page -> InsertRecord(index_name_, root_page_id_);
    } else {
      // update root_page_id in header_page
      header_page -> UpdateRecord(index_name_, root_page_id_);
    }
    buffer_pool_manager_ -> UnpinPage(HEADER_PAGE_ID, true);
  }

  /*
   * This method is used for test only
   * Read data from file and insert one by one
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::InsertFromFile(const std::string & file_name, Transaction * transaction) {
    int64_t key;
    std::ifstream input(file_name);
    while (input) {
      input >> key;

      KeyType index_key;
      index_key.SetFromInteger(key);
      RID rid(key);
      Insert(index_key, rid, transaction);
    }
  }
  /*
   * This method is used for test only
   * Read data from file and remove one by one
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::RemoveFromFile(const std::string & file_name, Transaction * transaction) {
    int64_t key;
    std::ifstream input(file_name);
    while (input) {
      input >> key;
      KeyType index_key;
      index_key.SetFromInteger(key);
      Remove(index_key, transaction);
    }
  }

  /**
   * This method is used for debug only, You don't need to modify
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::Draw(BufferPoolManager * bpm,
    const std::string & outf) {
    if (IsEmpty()) {
      LOG_WARN("Draw an empty tree");
      return;
    }
    std::ofstream out(outf);
    out << "digraph G {" << std::endl;
    ToGraph(reinterpret_cast < BPlusTreePage * > (bpm -> FetchPage(root_page_id_) -> GetData()), bpm, out);
    out << "}" << std::endl;
    out.flush();
    out.close();
  }

  /**
   * This method is used for debug only, You don't need to modify
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::Print(BufferPoolManager * bpm) {
    if (IsEmpty()) {
      LOG_WARN("Print an empty tree");
      return;
    }
    ToString(reinterpret_cast < BPlusTreePage * > (bpm -> FetchPage(root_page_id_) -> GetData()), bpm);
  }

  /**
   * This method is used for debug only, You don't need to modify
   * @tparam KeyType
   * @tparam ValueType
   * @tparam KeyComparator
   * @param page
   * @param bpm
   * @param out
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::ToGraph(BPlusTreePage * page, BufferPoolManager * bpm, std::ofstream & out) const {
    std::string leaf_prefix("LEAF_");
    std::string internal_prefix("INT_");
    if (page -> IsLeafPage()) {
      auto * leaf = reinterpret_cast < LeafPage * > (page);
      // Print node name
      out << leaf_prefix << leaf -> GetPageId();
      // Print node properties
      out << "[shape=plain color=green ";
      // Print data of the node
      out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
      // Print data
      out << "<TR><TD COLSPAN=\"" << leaf -> GetSize() << "\">P=" << leaf -> GetPageId() << "</TD></TR>\n";
      out << "<TR><TD COLSPAN=\"" << leaf -> GetSize() << "\">" <<
        "max_size=" << leaf -> GetMaxSize() << ",min_size=" << leaf -> GetMinSize() << ",size=" << leaf -> GetSize() <<
        "</TD></TR>\n";
      out << "<TR>";
      for (int i = 0; i < leaf -> GetSize(); i++) {
        out << "<TD>" << leaf -> KeyAt(i) << "</TD>\n";
      }
      out << "</TR>";
      // Print table end
      out << "</TABLE>>];\n";
      // Print Leaf node link if there is a next page
      if (leaf -> GetNextPageId() != INVALID_PAGE_ID) {
        out << leaf_prefix << leaf -> GetPageId() << " -> " << leaf_prefix << leaf -> GetNextPageId() << ";\n";
        out << "{rank=same " << leaf_prefix << leaf -> GetPageId() << " " << leaf_prefix << leaf -> GetNextPageId() << "};\n";
      }

      // Print parent links if there is a parent
      if (leaf -> GetParentPageId() != INVALID_PAGE_ID) {
        out << internal_prefix << leaf -> GetParentPageId() << ":p" << leaf -> GetPageId() << " -> " << leaf_prefix <<
          leaf -> GetPageId() << ";\n";
      }
    } else {
      auto * inner = reinterpret_cast < InternalPage * > (page);
      // Print node name
      out << internal_prefix << inner -> GetPageId();
      // Print node properties
      out << "[shape=plain color=pink "; // why not?
      // Print data of the node
      out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
      // Print data
      out << "<TR><TD COLSPAN=\"" << inner -> GetArraySize()  << "\">P=" << inner -> GetPageId() << "</TD></TR>\n";
      out << "<TR><TD COLSPAN=\"" << inner -> GetArraySize() << "\">" <<
        "max_size=" << inner -> GetMaxSize() << ",min_size=" << inner -> GetMinSize() << ",size=" << inner ->GetArraySize() <<
        "</TD></TR>\n";
      out << "<TR>";
      for (int i = 0; i < inner -> GetArraySize(); i++) {
        
        out << "<TD PORT=\"p" << inner -> ValueAt(i) << "\">";
        if (i > 0) {
          out << inner -> KeyAt(i);
        } else {
          out << " ";
        }
        out << "</TD>\n";
      }
      out << "</TR>";
      // Print table end
      out << "</TABLE>>];\n";
      // Print Parent link
      if (inner -> GetParentPageId() != INVALID_PAGE_ID) {
        out << internal_prefix << inner -> GetParentPageId() << ":p" << inner -> GetPageId() << " -> " << internal_prefix <<
          inner -> GetPageId() << ";\n";
      }
      // Print leaves
      for (int i = 0; i < inner -> GetArraySize(); i++) {
        auto child_page = reinterpret_cast < BPlusTreePage * > (bpm -> FetchPage(inner -> ValueAt(i)) -> GetData());
        ToGraph(child_page, bpm, out);
        if (i > 0) {
          auto sibling_page = reinterpret_cast < BPlusTreePage * > (bpm -> FetchPage(inner -> ValueAt(i - 1)) -> GetData());
          if (!sibling_page -> IsLeafPage() && !child_page -> IsLeafPage()) {
            out << "{rank=same " << internal_prefix << sibling_page -> GetPageId() << " " << internal_prefix <<
              child_page -> GetPageId() << "};\n";
          }
          bpm -> UnpinPage(sibling_page -> GetPageId(), false);
        }
      }
    }
    bpm -> UnpinPage(page -> GetPageId(), false);
  }

  /**
   * This function is for debug only, you don't need to modify
   * @tparam KeyType
   * @tparam ValueType
   * @tparam KeyComparator
   * @param page
   * @param bpm
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::ToString(BPlusTreePage * page, BufferPoolManager * bpm) const {
    if (page -> IsLeafPage()) {
      auto * leaf = reinterpret_cast < LeafPage * > (page);
      std::cout << "Leaf Page: " << leaf -> GetPageId() << " parent: " << leaf -> GetParentPageId() <<
        " next: " << leaf -> GetNextPageId() << std::endl;
      for (int i = 0; i < leaf -> GetSize(); i++) {
        std::cout << leaf -> KeyAt(i) << ",";
      }
      std::cout << std::endl;
      std::cout << std::endl;
    } else {
      auto * internal = reinterpret_cast < InternalPage * > (page);

      std::cout << "Internal Page: " << internal -> GetPageId() << " parent: " << internal -> GetParentPageId() << std::endl;
      for (int i = 0; i < internal -> GetSize() + 1; i++) {
        std::cout << internal -> KeyAt(i) << ": " << internal -> ValueAt(i) << ",";
      }
      std::cout << std::endl;
      std::cout << std::endl;
         
      for (int i = 0; i < internal -> GetSize() + 1; i++) {
        ToString(reinterpret_cast < BPlusTreePage * > (bpm -> FetchPage(internal -> ValueAt(i)) -> GetData()), bpm);
      }
    }
    bpm -> UnpinPage(page -> GetPageId(), false);
  }
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::testKeyExist(KeyComparator comparator) -> int {

    RID rid;
    RID rid2;
    KeyType index_key;
    KeyType index_key2;
    BPlusTreeInternalPage < KeyType, page_id_t, KeyComparator > internalPage = {};
    internalPage.Init((page_id_t) 1, (page_id_t) 0, 2);
    int64_t key = 42;
    int64_t key2 = 32;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast < int32_t > (key), value);
    rid2.Set(static_cast < int32_t > (key2), value);
    index_key.SetFromInteger(key);
    index_key2.SetFromInteger(key2);
    page_id_t pageId1 = 1;
    page_id_t pageId2 = 2;
    internalPage.Insert(pageId1, index_key, pageId2, comparator);
    return 2;
  }
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::GetInvalidPair() -> std::pair<KeyType, page_id_t> {
    KeyType k;
    page_id_t invalidPage = INVALID_PAGE_ID;
    return std::make_pair(k, invalidPage);

  }
  template class BPlusTree < GenericKey < 4 > , RID, GenericComparator < 4 >> ;
  template class BPlusTree < GenericKey < 8 > , RID, GenericComparator < 8 >> ;
  template class BPlusTree < GenericKey < 16 > , RID, GenericComparator < 16 >> ;
  template class BPlusTree < GenericKey < 32 > , RID, GenericComparator < 32 >> ;
  template class BPlusTree < GenericKey < 64 > , RID, GenericComparator < 64 >> ;

} // namespace bustub

//Parentid shout updated in insertToPArent

//Insertion in internal node is wrong