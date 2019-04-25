#include <iostream>
#include "wrapped_mapper.hpp"

int main() {
    WrappedMapper wm;
    wm.run_alignment();
    std::cout << wm;
    std::ofstream log("metrics.log");
    log << wm.prepare_log();
    log.close();
}