idf_build_get_property(target IDF_TARGET)


set(srcs            app_main.cpp)

set(src_dirs        .)

set(include_dirs    .)

idf_component_register(SRCS ${srcs} SRC_DIRS ${src_dirs} INCLUDE_DIRS ${include_dirs} REQUIRES ${requires})
