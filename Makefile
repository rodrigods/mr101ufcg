all: extra_code.tar.gz

# List all extra .py files you want here
extra_code.tar.gz: extra_code.py
	tar zcvf $@ $^


.PHONY:all clean

clean:
	rm -fv extra_code.tar.gz