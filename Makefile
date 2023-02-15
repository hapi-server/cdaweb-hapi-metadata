IDREGEX=^AC_AT

.PHONY: all bw jf nl bh compare

all: node_modules
	make bw
	make nl
	make bh
	make jf

compare:
	node compare.js

bw:
	node CDAS2HAPIall.js --idregex '$(IDREGEX)'

# Nand's Lal's (nl) production HAPI server
nl:
	node HAPI2HAPIall.js --version 'nl' --idregex '$(IDREGEX)'	

# Bernie Harris' (bh) prototype HAPI server
bh:
	node HAPI2HAPIall.js --version 'bh' --idregex '$(IDREGEX)'	


# Jeremy Faden's (jf) AutoplotDataServer HAPI server
ap-test:
	java -Djava.awt.headless=true -cp bin/autoplot.jar org.autoplot.AutoplotDataServer -q --uri='vap+cdaweb:ds=OMNI2_H0_MRG1HR&id=DST1800' -f hapi-info	

bin/autoplot.jar:
	@mkdir -p bin
	@cd bin; curl -O http://autoplot.org/devel/autoplot.jar

jf: bin/autoplot.jar
	node HAPI2HAPIall.js --version 'jf' --idregex '$(IDREGEX)'	

node_modules:
	npm install

distclean:
	@make clean
	@rm -rf node_modules/
	@rm -rf bin/

clean-bw:
	@rm -f all/all-bw.json
	@rm -f all/all-bw-full.json
	@rm -rf cache/bw/

clean-jf:
	@rm -f all/all-jf.json
	@rm -rf cache/jf/

clean-nl:
	@rm -f all/all-nl.json
	@rm -rf cache/nl

clean-bh:
	@rm -f all/all-bh.json
	@rm -rf cache/bh

clean:
	make clean-bw
	make clean-bh
	make clean-nl
	make clean-jf
	rm -f package-lock.json
