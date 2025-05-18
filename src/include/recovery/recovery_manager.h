#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
    lsn_t checkpoint_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase persist_data_{};

    inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

    inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
    /**
    * TODO: Student Implement
    */
    void Init(CheckPoint &last_checkpoint) {
        persist_lsn_ = last_checkpoint.checkpoint_lsn_;
        active_txns_ = last_checkpoint.active_txns_;
        data_ = last_checkpoint.persist_data_;
    }

    /**
    * TODO: Student Implement
    */
    void RedoPhase() {
        for (auto &[lsn, log_rec] : log_recs_) {
            if (lsn <= persist_lsn_) {
                continue;
            }

            active_txns_[log_rec->txn_id_] = log_rec->lsn_;

            switch(log_rec->type_) {
                case LogRecType::kInsert:
                    data_[log_rec->key_] = log_rec->val_;
                    break;
                case LogRecType::kDelete:
                    data_.erase(log_rec->key_);
                    break;
                case LogRecType::kUpdate:
                    data_.erase(log_rec->key_);
                    data_[log_rec->new_key_] = log_rec->new_val_;
                    break;
                case LogRecType::kBegin:
                    active_txns_[log_rec->txn_id_] = log_rec->lsn_;
                    break;
                case LogRecType::kCommit:
                    active_txns_.erase(log_rec->txn_id_);
                    break;
                case LogRecType::kAbort:
                    active_txns_.erase(log_rec->txn_id_);
                    if (log_rec->prev_lsn_ != INVALID_LSN) {
                        auto prev_log_rec = log_recs_[log_rec->prev_lsn_];
                        while (prev_log_rec->type_ != LogRecType::kBegin) {
                            if (prev_log_rec->type_ == LogRecType::kInsert) {
                                data_.erase(prev_log_rec->key_);
                            } else if (prev_log_rec->type_ == LogRecType::kUpdate) {
                                data_.erase(prev_log_rec->new_key_);
                                data_[prev_log_rec->key_] = prev_log_rec->val_;
                            } else if (prev_log_rec->type_ == LogRecType::kDelete) {
                                data_[prev_log_rec->key_] = prev_log_rec->val_;
                            }
                            prev_log_rec = log_recs_[prev_log_rec->prev_lsn_];
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }

    /**
    * TODO: Student Implement
    */
    void UndoPhase() {
        for (auto &[txn_id, lsn] : active_txns_) {
            lsn_t current_lsn = lsn;

            while (current_lsn != INVALID_LSN) {
                auto it = log_recs_.find(current_lsn);
                if (it == log_recs_.end()) {
                    break;
                }

                LogRecPtr log_rec = it->second;

                switch(log_rec->type_) {
                    case LogRecType::kInsert:
                        data_.erase(log_rec->key_);
                        break;
                    case LogRecType::kDelete:
                        data_[log_rec->key_] = log_rec->val_;
                        break;
                    case LogRecType::kUpdate:
                        data_.erase(log_rec->new_key_);
                        data_[log_rec->key_] = log_rec->val_;
                        break;
                    case LogRecType::kBegin:
                        current_lsn = INVALID_LSN;
                        continue;
                    default:
                        break;
                }

                current_lsn = log_rec->prev_lsn_;
            }
        }

        active_txns_.clear();
    }

    // used for test only
    void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

    // used for test only
    inline KvDatabase &GetDatabase() { return data_; }

private:
    std::map<lsn_t, LogRecPtr> log_recs_{};
    lsn_t persist_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
