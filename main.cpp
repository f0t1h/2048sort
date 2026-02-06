#include "order.hpp"
#include <array>
#include <charconv>

using stintp = std::pair<std::array<char, 16>, int>;
struct compair {
  bool operator()(const stintp &a, const stintp &b) const {
    return a.second < b.second; // Compare based on the integer value
  }
};

int main() {
  Order::Sorter2048<stintp, compair> manu(4, 40, "temp");
  // Genearate random numbers
  int N = 50'000'000;
  int BatchSize = 1'000'00;

  std::vector<std::thread> threads;
  for (int tid = 0; tid < 4; tid++) {
    threads.emplace_back([&manu, BatchSize, N, tid]() {
      std::vector<stintp> data;
      data.resize(BatchSize);
      for (int i = 0; i < N; i++) {
        data[i % BatchSize].second = rand() % BatchSize;
        data[i % BatchSize].first.fill(0);
        std::to_chars(data[i % BatchSize].first.data(),
                      data[i % BatchSize].first.data() + 16, i);

        if (i % BatchSize == (BatchSize - 1)) {
          manu.push(&data[0], &data[BatchSize]);
        }
      }
    });
  }
  for (auto &t : threads) {
    t.join();
  }

  std::string filename = manu.finish();
  manu.execute(
      [](const stintp &k) { printf("%s %d\n", k.first.data(), k.second); });

  return 0;
}
