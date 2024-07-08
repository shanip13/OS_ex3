# Compiler
CXX = g++

# Compiler flags
CXXFLAGS = -Wall -std=c++17

# Output library name
LIBRARY = libMapReduceFramework.a

# Source files
SRCS = MapReduceFramework.cpp Barrier.cpp

# Object files (derived from source files)
OBJS = $(SRCS:.cpp=.o)

# Default target
all: $(LIBRARY)

# Rule to create the static library
$(LIBRARY): $(OBJS)
	ar rcs $@ $^

# Rule to compile source files into object files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean target to remove compiled files
clean:
	rm -f $(OBJS) $(LIBRARY)

# Phony targets
.PHONY: all clean