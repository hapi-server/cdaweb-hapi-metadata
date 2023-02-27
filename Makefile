IDREGEX=^AC_OR
AUTOPLOT=http://autoplot.org/devel/autoplot.jar

.PHONY: all bw jf nl bh compare-meta

all: node_modules
	@mkdir -p all
	@echo "\n-----bw------\n"
	make bw
	@echo "\n-----nl------\n"
	make nl
	@echo "\n-----bh------\n"
	make bh
	@echo "\n-----jf------\n"
	make jf
	@echo "\n-----compare-meta------\n"
	make compare-meta

CDAS="https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/"
inventory:
	curl "$(CDAS)dataviews/sp_phys/datasets/AC_H2_MFI/inventory/19970829T000000Z,19970928T100010Z"
	# Returns
	# <Start>1997-09-02T00:00:00.000Z</Start><End>1997-09-28T23:00:00.000Z</End>

rsync:
	rsync -avz  --delete cache weigel@mag.gmu.edu:www/git-data/cdaweb-hapi-metadata
	rsync -avz  --delete all weigel@mag.gmu.edu:www/git-data/cdaweb-hapi-metadata

compare-meta:
	cd compare && mkdir -p meta && node compare-meta.js

#cp cache/bw/all.json all/all-bw.json
bw:
	node CDAS2HAPIall.js --idregex '$(IDREGEX)'

bwm:
	node MASTERS2HAPIall.js --idregex '$(IDREGEX)'

# Nand's Lal's (nl) production HAPI server
nl:
	node HAPI2HAPIall.js --version 'nl' --idregex '$(IDREGEX)'

nljf:
	node HAPI2HAPIall.js --maxsockets 5 --version 'nljf' --idregex '$(IDREGEX)' --hapiurl 'https://jfaden.net/server/cdaweb/hapi'

# Bernie Harris' (bh) prototype HAPI server
bh:
	node HAPI2HAPIall.js --version 'bh' --idregex '$(IDREGEX)'	


# Jeremy Faden's (jf) AutoplotDataServer HAPI server
ap-test:
	java -Djava.awt.headless=true -cp bin/autoplot.jar org.autoplot.AutoplotDataServer -q --uri='vap+cdaweb:ds=OMNI2_H0_MRG1HR&id=DST1800' -f hapi-info	

bin/autoplot.jar:
	@mkdir -p bin
	@echo "Downloading $(AUTOPLOT)"
	@cd bin; curl -s -O $(AUTOPLOT)

jf: bin/autoplot.jar
	node HAPI2HAPIall.js --version 'jf' --idregex '$(IDREGEX)'	

node_modules:
	npm install

distclean:
	@make clean
	@rm -rf node_modules/
	@rm -rf bin/
	@rm -rf all/

clean-bw:
	@rm -f all/all-bw.json
	@rm -f all/all-bw-full.json
	@rm -rf cache/bw/

clean-bwm:
	@rm -f all/all-bwm.json
	@rm -f all/all-bwm-full.json
	@rm -rf cache/bwm/

clean-jf:
	@rm -f all/all-jf.json
	@rm -rf cache/jf/

clean-nl:
	@rm -f all/all-nl.json
	@rm -rf cache/nl

clean-nljf:
	@rm -f all/all-nljf.json
	@rm -rf cache/nljf

clean-bh:
	@rm -f all/all-bh.json
	@rm -rf cache/bh

clean:
	make clean-bw
	make clean-bwm
	make clean-bh
	make clean-nl
	make clean-nljf
	make clean-jf
	rm -f package-lock.json
