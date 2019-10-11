#!/bin/bash
# Using clang-format and gofmt to format c/c++/golang source code.

formatByClangFormat() {
	for file in $(find $1 -regextype "posix-egrep" -regex ".*[A-Za-z_]\.(h|hpp|cc)")
	do
		COMMAND="clang-format -i $file"
		echo $COMMAND
		eval $COMMAND
	done
}

if command -v clang-format >/dev/null; then
    DIRS=( ./include ./examples )
	for DIR in "${DIRS[@]}"
	do
		formatByClangFormat $DIR
	done
else
	echo "[ERROR] clang-format is required to format c/c++ code"
fi
