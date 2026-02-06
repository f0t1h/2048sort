#ifndef _ORDER_D_H
#define _ORDER_D_H

#include <concurrentqueue.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <queue>
#include <set>
#include <string>
#include <thread>

namespace Order {

// a class K and its comparator C
template <class K, class C> struct Sorter2048 {
  struct BatchEntry {
    K *from;
    K *to;
  };
  struct Job {
    int id;
    int level;
    bool operator<(const Job &other) const {
      if (level == other.level) {
        return id > other.id; // Higher ID has lower priority
      }
      return level < other.level; // Higher level has lower priority
    }
    std::string filename() const {
      return std::to_string(id) + "_" + std::to_string(level) + ".tmp";
    }
  };

  // Helper function to read a single item from a binary stream
  static bool read_item(std::ifstream &in, K &item) {
    return static_cast<bool>(in.read(reinterpret_cast<char *>(&item), sizeof(K)));
  }

  // Helper function to write a single item to a binary stream
  static void write_item(std::ofstream &out, const K &item) {
    out.write(reinterpret_cast<const char *>(&item), sizeof(K));
  }

  // Batched reader for efficient file reading
  struct BatchedReader {
    std::ifstream &file;
    K *buffer;
    size_t buffer_size;
    size_t pos;
    size_t count;
    bool exhausted;

    BatchedReader(std::ifstream &f, K *buf, size_t buf_size)
        : file(f), buffer(buf), buffer_size(buf_size), pos(0), count(0), exhausted(false) {
      refill();
    }

    void refill() {
      if (exhausted) return;
      file.read(reinterpret_cast<char *>(buffer), buffer_size * sizeof(K));
      size_t bytes_read = file.gcount();
      count = bytes_read / sizeof(K);
      pos = 0;
      if (count == 0) {
        exhausted = true;
      }
    }

    bool has_more() const { return !exhausted; }

    const K &current() const { return buffer[pos]; }

    void advance() {
      pos++;
      if (pos >= count) {
        refill();
      }
    }
  };

  // Batched writer for efficient file writing
  struct BatchedWriter {
    std::ofstream &file;
    K *buffer;
    size_t buffer_size;
    size_t pos;

    BatchedWriter(std::ofstream &f, K *buf, size_t buf_size)
        : file(f), buffer(buf), buffer_size(buf_size), pos(0) {}

    void write(const K &item) {
      buffer[pos++] = item;
      if (pos >= buffer_size) {
        flush();
      }
    }

    void flush() {
      if (pos > 0) {
        file.write(reinterpret_cast<const char *>(buffer), pos * sizeof(K));
        pos = 0;
      }
    }
  };

  // Merge two in-memory sorted ranges and write to file
  static void merge_to_file(std::ofstream &out, K *&b1, K *b1_end, K *&b2, K *b2_end) {
    while (b1 < b1_end && b2 < b2_end) {
      if (C()(*b1, *b2)) {
        write_item(out, *b1++);
      } else {
        write_item(out, *b2++);
      }
    }
    while (b1 < b1_end) {
      write_item(out, *b1++);
    }
    while (b2 < b2_end) {
      write_item(out, *b2++);
    }
  }

  // Merge two sorted files into an output file
  static bool merge_files(std::ifstream &f1, std::ifstream &f2, std::ofstream &out) {
    K k1, k2;
    bool has_k1 = read_item(f1, k1);
    bool has_k2 = read_item(f2, k2);
    while (has_k1 && has_k2) {
      if (C()(k1, k2)) {
        write_item(out, k1);
        has_k1 = read_item(f1, k1);
      } else {
        write_item(out, k2);
        has_k2 = read_item(f2, k2);
      }
    }
    while (has_k1) {
      write_item(out, k1);
      has_k1 = read_item(f1, k1);
    }
    while (has_k2) {
      write_item(out, k2);
      has_k2 = read_item(f2, k2);
    }
    return true;
  }

  // Write a batch entry to file
  static void write_batch_to_file(std::ofstream &out, K *from, K *to) {
    while (from < to) {
      write_item(out, *from++);
    }
  }

  Sorter2048(int threads, long long maxMem, const std::string &workdir)
      : done(false), threads(threads), maxMem(maxMem), workdir(workdir),
        work_file_prefix{workdir + "/B"}, push_queue(10,threads, 1),
        managerThread{std::thread{[this]() { manage_sorting(); }}} {
    std::filesystem::create_directories(workdir);
  }
  ~Sorter2048() {
    done.store(true);
    if (managerThread.joinable()) {
      managerThread.join();
    }

  }

  void manage_sorting() {
    // printf("Nice\n");
    // printf("Threads: %d\nMaximum Memory: %lld\n", threads, maxMem);

    while (!done.load()) {
      BatchEntry job;
      if (push_queue.try_dequeue(job)) {
        std::sort(job.from, job.to, C());
        waitroom.push(job);
      }
      if (waitroom.size() > 1) {
        // Merge sorted entries and write to disk
        Job j{job_idx++, 0};
        std::ofstream of{work_file_prefix + j.filename(), std::ios::binary};
        if (!of) {
          std::cerr << "Failed to open file for writing: "
                    << work_file_prefix + j.filename() << std::endl;
          continue;
        }
        BatchEntry b1 = waitroom.front();
        K *b1f = b1.from;
        waitroom.pop();
        BatchEntry b2 = waitroom.front();
        K *b2f = b2.from;
        waitroom.pop();
        merge_to_file(of, b1.from, b1.to, b2.from, b2.to);
        delete[] b1f;
        delete[] b2f;
        JQ.insert(j);
      }
      while (JQ.size() > 1) {
        auto first = JQ.begin();
        auto second = std::next(first);
        if (second != JQ.end() && first->level == second->level) {
          // Store job info before erasing iterators
          Job job1 = *first;
          Job job2 = *second;
          Job merged{job_idx++, job1.level + 1};

          // Erase iterators first to avoid invalidation issues
          JQ.erase(second);
          JQ.erase(first);

          std::ofstream of{work_file_prefix + merged.filename(),
                           std::ios::binary};
          if (!of) {
            std::cerr << "Failed to open file for writing: "
                      << work_file_prefix + merged.filename() << std::endl;
            // Re-insert jobs since we couldn't merge
            JQ.insert(job1);
            JQ.insert(job2);
            continue;
          }
          std::ifstream f1{work_file_prefix + job1.filename(),
                           std::ios::binary};
          std::ifstream f2{work_file_prefix + job2.filename(),
                           std::ios::binary};
          if (!f1 || !f2) {
            std::cerr << "Failed to open file for reading: "
                      << work_file_prefix + job1.filename() << " or "
                      << work_file_prefix + job2.filename() << std::endl;
            // Re-insert jobs since we couldn't merge
            JQ.insert(job1);
            JQ.insert(job2);
            continue;
          }
          merge_files(f1, f2, of);
          std::filesystem::remove(work_file_prefix + job1.filename());
          std::filesystem::remove(work_file_prefix + job2.filename());
          JQ.insert(merged);
        }
        else{
            break;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  std::string finish() {
    done.store(true);
    managerThread.join();
    // Empty Waitrom
    while (push_queue.size_approx() > 0) {
      BatchEntry job;
      if (push_queue.try_dequeue(job)) {
        std::sort(job.from, job.to, C());
        waitroom.push(job);
      }
    }
    while (waitroom.size() > 0) {
      BatchEntry b = waitroom.front();
      K * bf = b.from;
      waitroom.pop();
      Job j{job_idx++, 0};
      std::ofstream of{work_file_prefix + j.filename(), std::ios::binary};
      if (!of) {
        std::cerr << "Failed to open file for writing: "
                  << work_file_prefix + j.filename() << std::endl;
        continue;
      }
      write_batch_to_file(of, b.from, b.to);
      JQ.insert(j);
      delete[] bf;
    }
    while (JQ.size() > 1) {
      auto first = JQ.begin();
      auto second = std::next(first);
      if (second == JQ.end()) break;

      // Store job info before erasing
      Job job1 = *first;
      Job job2 = *second;

      int tgt_level = std::max(job1.level, job2.level);
      if (job1.level == job2.level) {
          tgt_level++;
      }
      fprintf(stderr, "Merging files: %s and %s into level %d\n",
              job1.filename().c_str(), job2.filename().c_str(), tgt_level);

      // Erase iterators first
      JQ.erase(second);
      JQ.erase(first);

      Job merged{job_idx++, tgt_level};
      std::ofstream of{work_file_prefix + merged.filename(), std::ios::binary};
      if (!of) {
        std::cerr << "Failed to open file for writing: "
                  << work_file_prefix + merged.filename() << std::endl;
        JQ.insert(job1);
        JQ.insert(job2);
        continue;
      }
      std::ifstream f1{work_file_prefix + job1.filename(), std::ios::binary};
      std::ifstream f2{work_file_prefix + job2.filename(), std::ios::binary};
      if (!f1 || !f2) {
        std::cerr << "Failed to open file for reading: "
                  << work_file_prefix + job1.filename() << " or "
                  << work_file_prefix + job2.filename() << std::endl;
        JQ.insert(job1);
        JQ.insert(job2);
        continue;
      }
      merge_files(f1, f2, of);
      std::filesystem::remove(work_file_prefix + job1.filename());
      std::filesystem::remove(work_file_prefix + job2.filename());
      JQ.insert(merged);
    }
    return work_file_prefix + JQ.begin()->filename();
  }
  template <class F> void execute(const F &f) {
    std::string file = work_file_prefix + JQ.begin()->filename();
    std::ifstream in{file, std::ios::binary};
    if (!in) {
      std::cerr << "Failed to open file for reading: " << file << std::endl;
      return;
    }
    K k;
    while (read_item(in, k)) {
      f(k);
    }

  }
  void push(K *from, K *to) {
    // Copy first
    BatchEntry be{new K[std::distance(from, to)], 0};
    std::copy(from, to, be.from);
    be.to = be.from + std::distance(from, to);


    while(!push_queue.try_enqueue(be)){
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  std::atomic<bool> done;
  int threads;
  long long maxMem;
  std::string workdir;
  std::string work_file_prefix;
  std::queue<BatchEntry> waitroom;
  moodycamel::ConcurrentQueue<BatchEntry> push_queue;
  std::multiset<Job> JQ;
  int job_idx = 0;

  //Keep at the end
  std::thread managerThread;
};
} // namespace Order

#endif
