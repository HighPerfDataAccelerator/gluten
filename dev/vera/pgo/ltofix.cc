#include <system_error>
// Emit out-of-line weak definition needed by non-LTO folly objects (LTO inlined/dropped it).
void ltofix_use(std::error_code e, const char* s) { throw std::system_error(e, s); }
