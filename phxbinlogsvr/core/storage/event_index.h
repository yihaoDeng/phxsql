/*
	Tencent is pleased to support the open source community by making PhxSQL available.
	Copyright (C) 2016 THL A29 Limited, a Tencent company. All rights reserved.
	Licensed under the GNU General Public License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
	
	https://opensource.org/licenses/GPL-2.0
	
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/

#pragma once

#include "rocksdb/db.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "eventdata.pb.h"

using rocksdb::Status;
namespace phxbinlog {

class ScopeSnapshot {
  public:
    ScopeSnapshot(rocksdb::DB* db, const rocksdb::Snapshot** snapshot) :
      db_(db), snapshot_(snapshot) {
        *snapshot_ = db_->GetSnapshot();

      }
    ~ScopeSnapshot() {
      db_->ReleaseSnapshot(*snapshot_);

    }
  private:
    rocksdb::DB* const db_;
    const rocksdb::Snapshot** snapshot_;
    ScopeSnapshot(const ScopeSnapshot&);
    void operator=(const ScopeSnapshot&);
};

class EventComparatorImpl : public rocksdb::Comparator {
  public: 
    const char *Name() const override {
      return "phxbinlogsrv.EventComparatorImpl";
    } 
    int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override; 
    void FindShortestSeparator(std::string *, const rocksdb::Slice &) const {
    }
    void FindShortSuccessor(std::string *) const {
    }
};

class EventIndex {
  public:
    EventIndex(const std::string &event_path);
    ~EventIndex();

    Status GetGTID(const std::string &gtid, ::google::protobuf::Message *data_info);
    Status GetLowerBoundGTID(const std::string &gtid, ::google::protobuf::Message *data_info);

    Status SetGTIDIndex(const std::string &gtid, const ::google::protobuf::Message &data_info);
    Status DeleteGTIDIndex(const std::string &gtid);
    Status IsExist(const std::string &gtid);

  private:
    void CloseDB();
    Status OpenDB(const std::string &event_path);
    Status GetGTIDIndex(const std::string &gtid, ::google::protobuf::Message *data_info,
        bool lower_bound);
  private:
    rocksdb::DB *db_;
};

}

