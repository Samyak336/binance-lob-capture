// replaces global new/delete to count allocations, so a test can check that
// processing messages doesn't allocate once warmed up.
// - has to be its own file: if gcc can inline these into the callers it throws a
//   bogus -Wmismatched-new-delete warning
// - has to replace the array and nothrow versions too, otherwise ASan's own
//   new[] gets paired with our delete and reports a mismatch

#include <cstddef>
#include <cstdlib>
#include <new>

static size_t allocations = 0;

size_t allocationCount() { return allocations; }

static void* allocate(size_t n) {
    allocations++;
    return std::malloc(n ? n : 1);
}

void* operator new(size_t n) {
    if (void* p = allocate(n)) return p;
    throw std::bad_alloc();
}
void* operator new[](size_t n) {
    if (void* p = allocate(n)) return p;
    throw std::bad_alloc();
}
void* operator new(size_t n, const std::nothrow_t&) noexcept { return allocate(n); }
void* operator new[](size_t n, const std::nothrow_t&) noexcept { return allocate(n); }

void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, size_t) noexcept { std::free(p); }
void operator delete[](void* p, size_t) noexcept { std::free(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }
