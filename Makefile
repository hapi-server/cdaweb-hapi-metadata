IDREGEX=^AC_AT

all: node_modules
	make bw
	make nl
	make bh

# Install nodejs dependencies
node_modules:
	npm install

bw:
	node CDAS2HAPIall.js --idregex '$(IDREGEX)'

# Jeremy Faden's (jf) AutoplotDataServer HAPI server
jf:
	node HAPI2HAPIall.js --version 'jf' --idregex '$(IDREGEX)'	

# Nand's Lal's (nl) production HAPI server
nl:
	node HAPI2HAPIall.js --version 'nl' --idregex '$(IDREGEX)'	

# Bernie Harris' (bh) prototype HAPI server
bh:
	node HAPI2HAPIall.js --version 'bh' --idregex '$(IDREGEX)'	

compare:
	node compare.js

distclean:
	@make clean
	@rm -rf node_modules/

clean-bw:
	@rm -f all-bw.json
	@rm -f all-bw-full.json
	@rm -rf cache/bw/

clean-jf:
	@rm -f all-jf.json
	@rm -rf cache/jf/

clean-nl:
	@rm -f all-nl.json
	@rm -rf cache/nl

clean-bh:
	@rm -f all-bh.json
	@rm -rf cache/bh

clean:
	make clean-bw
	make clean-bh
	make clean-nl
	make clean-jf
	rm -f package-lock.json
