add_subdirectory(cpr)

# This ensures that things like gnu++11 get passed correctly
set(CMAKE_CXX_STANDARD 11)

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(APPLE)
  set(REDIS_MODULE_CFLAGS -W -Wall -dynamic -fno-common -g -ggdb -std=c++11 -O2)
  set(REDIS_MODULE_LDFLAGS "-undefined dynamic_lookup")
  set(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
else()
  set(REDIS_MODULE_CFLAGS -W -Wall -fno-common -g -ggdb -std=c++11 -O2)
  set(REDIS_MODULE_LDFLAGS -shared)
endif()

include_directories(${CPR_INCLUDE_DIRS})

add_library(credis SHARED credis.cc)
target_compile_options(credis PUBLIC ${REDIS_MODULE_CFLAGS} -fPIC)
target_link_libraries(credis ${REDIS_MODULE_LDFLAGS} ${CPR_LIBRARIES})
