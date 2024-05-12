#pragma once

#include <algorithm>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#define get_key first
#define get_value second

template <class K, class V, class Hash = std::hash<K>>
class ConcurrentHashMap {
public:
    ConcurrentHashMap(const Hash& hasher = Hash()) {
        hash_function_ = hasher;
        mutexes_.resize(count_threads_);
        storage_.resize(count_threads_, std::vector<std::vector<std::pair<K, V>>>(
                                            storage_size_, std::vector<std::pair<K, V>>()));
    }

    explicit ConcurrentHashMap(int expected_size, const Hash& hasher = Hash()) {
        hash_function_ = hasher;
        storage_size_ =
            std::max<size_t>(storage_size_, (expected_size + count_threads_ - 1) / count_threads_);
        mutexes_.resize(count_threads_);
        storage_.resize(count_threads_, std::vector<std::vector<std::pair<K, V>>>(
                                            storage_size_, std::vector<std::pair<K, V>>()));
    }

    ConcurrentHashMap(int expected_size, int expected_threads_count, const Hash& hasher = Hash()) {
        hash_function_ = hasher;
        count_threads_ = std::min<size_t>(count_threads_, expected_threads_count);
        storage_size_ =
            std::max<size_t>(storage_size_, (expected_size + count_threads_ - 1) / count_threads_);
        mutexes_.resize(count_threads_);
        storage_.resize(count_threads_, std::vector<std::vector<std::pair<K, V>>>(
                                            storage_size_, std::vector<std::pair<K, V>>()));
    }

    size_t GetHash(const K& key, const size_t mod) const {
        return hash_function_(key) % mod;
    }

    void Rehashing() {
        for (size_t i = 0; i < count_threads_; ++i) {
            mutexes_[i].lock();
        }
        ConcurrentHashMap hash_map(storage_size_ * count_threads_ * 3, count_threads_,
                                   hash_function_);
        for (size_t extern_index = 0; extern_index < count_threads_; ++extern_index) {
            for (size_t intern_index = 0; intern_index < storage_size_; ++intern_index) {
                for (size_t i = 0; i < storage_[extern_index][intern_index].size(); ++i) {
                    size_t new_ex_ind =
                        GetHash(storage_[extern_index][intern_index][i].get_key, count_threads_);
                    size_t new_in_ind = GetHash(storage_[extern_index][intern_index][i].get_key,
                                                hash_map.storage_size_);
                    hash_map.storage_[new_ex_ind][new_in_ind].emplace_back(
                        std::make_pair(storage_[extern_index][intern_index][i].get_key,
                                       storage_[extern_index][intern_index][i].get_value));
                }
            }
        }
        storage_ = hash_map.storage_;
        storage_size_ = (storage_size_ * count_threads_ * 3 + count_threads_ - 1) / count_threads_;
        for (size_t i = count_threads_; i > 0; --i) {
            mutexes_[i - 1].unlock();
        }
    }

    bool Insert(const K& key, const V& value) {
        size_t extern_index = GetHash(key, count_threads_);
        {
            std::lock_guard<std::mutex> lock(mutexes_[extern_index]);
            size_t intern_index = GetHash(key, storage_size_);
            size_t index = 0;
            while (index < storage_[extern_index][intern_index].size()) {
                if (storage_[extern_index][intern_index][index++].get_key == key) {
                    return false;
                }
            }
            ++size_;
            storage_[extern_index][intern_index].emplace_back(std::make_pair(key, value));
            if (storage_[extern_index][intern_index].size() < kLimitCollisions) {
                return true;
            }
        }
        Rehashing();
        return true;
    }

    bool Erase(const K& key) {
        size_t extern_index = GetHash(key, count_threads_);
        std::lock_guard<std::mutex> lock(mutexes_[extern_index]);
        size_t intern_index = GetHash(key, storage_size_);
        for (size_t i = 0; i < storage_[extern_index][intern_index].size(); ++i) {
            if (storage_[extern_index][intern_index][i].get_key == key) {
                --size_;
                std::swap(storage_[extern_index][intern_index][i],
                          storage_[extern_index][intern_index].back());
                storage_[extern_index][intern_index].pop_back();
                return true;
            }
        }
        return false;
    }

    void Clear() {
        size_ = 0;
        for (size_t i = 0; i < count_threads_; ++i) {
            mutexes_[i].lock();
        }
        storage_.clear();
        storage_size_ = kDefaultSize;
        storage_.resize(count_threads_, std::vector<std::vector<std::pair<K, V>>>(
                                            storage_size_, std::vector<std::pair<K, V>>()));
        for (size_t i = count_threads_; i > 0; --i) {
            mutexes_[i - 1].unlock();
        }
    }

    std::pair<bool, V> Find(const K& key) const {
        size_t extern_index = GetHash(key, count_threads_);
        std::lock_guard<std::mutex> lock(mutexes_[extern_index]);
        size_t intern_index = GetHash(key, storage_size_);
        for (size_t i = 0; i < storage_[extern_index][intern_index].size(); ++i) {
            if (storage_[extern_index][intern_index][i].get_key == key) {
                return std::make_pair(true, storage_[extern_index][intern_index][i].get_value);
            }
        }
        return std::make_pair(false, V());
    }

    const V At(const K& key) const {
        size_t extern_index = GetHash(key, count_threads_);
        std::lock_guard<std::mutex> lock(mutexes_[extern_index]);
        size_t intern_index = GetHash(key, storage_size_);
        for (size_t i = 0; i < storage_[extern_index][intern_index].size(); ++i) {
            if (storage_[extern_index][intern_index][i].get_key == key) {
                return storage_[extern_index][intern_index][i].get_value;
            }
        }
        throw std::out_of_range("this key has not value");
    }

    size_t Size() const {
        return size_;
    }

    static const int kDefaultConcurrencyLevel;
    static const int kUndefinedSize;
    static const size_t kDefaultSize;
    static const size_t kLimitCollisions;

private:
    size_t count_threads_ = kDefaultConcurrencyLevel;
    std::atomic<size_t> storage_size_ = kDefaultSize;
    std::atomic<size_t> size_ = 0;
    Hash hash_function_;
    mutable std::deque<std::mutex> mutexes_;
    std::vector<std::vector<std::vector<std::pair<K, V>>>> storage_;
};

template <class K, class V, class Hash>
const size_t ConcurrentHashMap<K, V, Hash>::kDefaultSize = 29;

template <class K, class V, class Hash>
const int ConcurrentHashMap<K, V, Hash>::kDefaultConcurrencyLevel =
    std::thread::hardware_concurrency();

template <class K, class V, class Hash>
const int ConcurrentHashMap<K, V, Hash>::kUndefinedSize = -1;

template <class K, class V, class Hash>
const size_t ConcurrentHashMap<K, V, Hash>::kLimitCollisions = 25;
