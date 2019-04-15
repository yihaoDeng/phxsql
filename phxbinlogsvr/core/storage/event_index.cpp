/*
	Tencent is pleased to support the open source community by making PhxSQL available.
	Copyright (C) 2016 THL A29 Limited, a Tencent company. All rights reserved.
	Licensed under the GNU General Public License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
	
	https://opensource.org/licenses/GPL-2.0
	
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/


#include "event_index.h"

#include "phxcomm/phx_log.h"
#include "phxbinlogsvr/core/gtid_handler.h"

using std::string;
using std::pair;
using phxsql::LogVerbose;
using phxsql::ColorLogError;
using phxsql::ColorLogWarning;

using rocksdb::Status; 

namespace phxbinlog {

int EventComparatorImpl::Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const {
  pair < string, size_t > key1 = GtidHandler::ParseGTID(a.ToString());
  pair < string, size_t > key2 = GtidHandler::ParseGTID(b.ToString());

  if (key1.first == key2.first) {
    if (key1.second == key2.second)
      return 0;
    return key1.second < key2.second ? -1 : 1;
  }

  return key1.first < key2.first ? -1 : 1;
}
rocksdb::Comparator* EnvetIndexComparator() {
  static EventComparatorImpl event_index_comparator;
  return &event_index_comparator;
}

EventIndex::EventIndex(const string &event_path) {
  OpenDB(event_path);
}
EventIndex::~EventIndex() {
  CloseDB();
}

Status EventIndex::OpenDB(const string &event_path) {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.comparator = EnvetIndexComparator();
  rocksdb::Status status = rocksdb::DB::Open(options, event_path, &db_);
   
  if (!status.ok()) {
    ColorLogError("%s path %s open db fail, err %s", __func__, event_path.c_str(), status.ToString().c_str());
  }
  assert(status.ok());
  return status;
}

void EventIndex::CloseDB() {
  if (db_)
    delete db_;
}

Status EventIndex::SetGTIDIndex(const string &gtid, const ::google::protobuf::Message &data_info) {
  string buffer;
  if (!data_info.SerializeToString(&buffer)) {
    ColorLogError("buffer serialize fail");
    return Status::Corruption("Buffer Serialize Fail");
  }
  return db_->Put(rocksdb::WriteOptions(), gtid, buffer);
}

Status EventIndex::GetGTID(const string &gtid, ::google::protobuf::Message *data_info) {
  return GetGTIDIndex(gtid, data_info, false);
}

Status EventIndex::GetLowerBoundGTID(const string &gtid, 
    ::google::protobuf::Message *data_info) {
  return GetGTIDIndex(gtid, data_info, true);
}

Status EventIndex::GetGTIDIndex(const string &gtid, ::google::protobuf::Message *data_info,
    bool lower_bound) {
  string buffer;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false; 
  rocksdb::Iterator* it = db_->NewIterator(iterator_options);
  if (!it->Valid()) {
    return Status::Corruption("No Gtid Found");
  }

  it->Seek(gtid);
  if (it->Valid()) {
    if (it->key().ToString() == gtid
        || (lower_bound && GtidHandler::GetUUIDByGTID(it->key().ToString()) == GtidHandler::GetUUIDByGTID(gtid))) {

      buffer = it->value().ToString();
      if (buffer.size()) {
        if (!data_info->ParseFromString(buffer)) {
          ColorLogWarning("%s from buffer fail data size %zu, want gtid %s", __func__, buffer.size(),
              gtid.c_str());
          return Status::Corruption("Data Error");
        } else {
          LogVerbose("%s gtid %s ok, get gtid %s, want gtid %s", __func__, gtid.c_str(),
              it->key().ToString().c_str(), gtid.c_str());
          return Status::OK(); 
        }
      } else {
        ColorLogWarning("%s get gtid %s, not exist", __func__, gtid.c_str());
      }
    } else {
      ColorLogWarning("%s get gtid %s, not exist", __func__, gtid.c_str());
    }
  }
  delete it;
  return Status::OK();
}

Status EventIndex::IsExist(const string &gtid) {
  string buffer;
  return db_->Get(rocksdb::ReadOptions(), gtid, &buffer);
}

Status EventIndex::DeleteGTIDIndex(const string &gtid) {
  return db_->Delete(rocksdb::WriteOptions(), gtid);
}

}
