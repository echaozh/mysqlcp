.PHONY: all install install-dbg clean

all: debug release-build

debug: build/debug/Makefile
	make -C build/debug

release-build: build/release/Makefile
	make -C build/release

install: release-build
	make -C build/release install

install-dbg: debug
	make -C build/debug install

clean:
	rm -rf build install install-dbg

build/debug/Makefile: | build/debug
	cd build/debug; \
	  cmake -DCMAKE_BUILD_TYPE=Debug \
	  -DCMAKE_INSTALL_PREFIX=$$PWD/../../install-dbg ../..

build/release/Makefile: | build/release
	cd build/release; \
	  cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ../..

build/debug:
	mkdir -p $@

build/release:
	mkdir -p $@
