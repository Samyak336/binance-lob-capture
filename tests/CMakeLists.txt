add_executable(unit_tests unit_tests.cpp alloc_counter.cpp)
target_link_libraries(unit_tests PRIVATE blc_core)
target_compile_options(unit_tests PRIVATE ${BLC_WARNINGS})
add_test(NAME unit_tests COMMAND unit_tests)

# `cmake --build build --target tests` builds ./build/tests/unit_tests
add_custom_target(tests DEPENDS unit_tests)
