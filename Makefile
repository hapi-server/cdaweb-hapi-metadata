IDREGEX=^AC_OR
CDAS="https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/"
#AUTOPLOT=http://autoplot.org/devel/autoplot.jar
AUTOPLOT=https://ci-pw.physics.uiowa.edu/job/autoplot-release-2022/lastSuccessfulBuild/artifact/autoplot/Autoplot/dist/autoplot.jar

.PHONY: all bw jf nl bh compare-meta compare-data

all: node_modules bin/autoplot.jar
	@mkdir -p hapi
	@echo "\n-----bw------\n"
	make bw IDREGEX=$(IDREGEX)
	@echo "\n-----nl------\n"
	make nl IDREGEX=$(IDREGEX)
	@echo "\n-----bh------\n"
	make bh IDREGEX=$(IDREGEX)
	#@echo "\n-----jf------\n"
	#make jf IDREGEX=$(IDREGEX)
	@echo "\n-----compare-meta------\n"
	make compare-meta IDREGEX=$(IDREGEX) && cat compare/compare-meta.json
	make compare-data IDREGEX=$(IDREGEX)

rsync:
	rsync -avz --delete cache weigel@mag.gmu.edu:www/git-data/cdaweb-hapi-metadata
	rsync -avz --delete hapi weigel@mag.gmu.edu:www/git-data/cdaweb-hapi-metadata
	rsync -avz --delete verify/data weigel@mag.gmu.edu:www/git-data/cdaweb-hapi-metadata/verify

compare-meta:
	mkdir -p compare/meta && node compare-meta.js

compare-data:
	mkdir -p compare/data && node compare-data.js

bw:
	node CDAS2HAPIinfo.js --idregex '$(IDREGEX)'

# Nand's Lal's (nl) production HAPI server
nl:
	node HAPIinfo.js --version 'nl' --idregex '$(IDREGEX)'

# Jeremy Faden's (jf) test version of nl's server
nljf:
	node HAPIinfo.js --version 'nljf' --idregex '$(IDREGEX)' --hapiurl 'https://jfaden.net/server/cdaweb/hapi'

# Bernie Harris' (bh) prototype HAPI server
bh:
	node HAPIinfo.js --version 'bh' --idregex '$(IDREGEX)'	

# Jeremy Faden's (jf) AutoplotDataServer HAPI server
jf: bin/autoplot.jar
	node HAPIinfo.js --version 'jf' --idregex '$(IDREGEX)'	

jf-test:
	java -Djava.awt.headless=true -cp bin/autoplot.jar org.autoplot.AutoplotDataServer -q --uri='vap+cdaweb:ds=OMNI2_H0_MRG1HR&id=DST1800' -f hapi-info	

jf-test-data:
	java -Djava.awt.headless=true -cp bin/autoplot.jar org.autoplot.AutoplotDataServer -q --uri='vap+cdaweb:ds=OMNI2_H0_MRG1HR&id=DST1800&timerange=2020-01-01T00Z/2020-02-01T00Z' -f hapi-data	

bin/autoplot.jar:
	@mkdir -p bin
	@echo "Downloading $(AUTOPLOT)"
	@cd bin; curl -s -O $(AUTOPLOT)

node_modules:
	npm install

distclean:
	@make clean
	@rm -rf node_modules/
	@rm -rf bin/
	@rm -rf hapi/

clean-bw:
	@rm -rf hapi/bw/
	@rm -rf cache/bw/

clean-jf:
	@rm -rf hapi/jf/
	@rm -rf cache/jf/

clean-nl:
	@rm -rf hapi/nl/
	@rm -rf cache/nl/

clean-nljf:
	@rm -rf hapi/nljf/
	@rm -rf cache/nljf/

clean-bh:
	@rm -rf hapi/bh/
	@rm -rf cache/bh/

clean:
	make clean-bw
	make clean-bh
	make clean-nl
	make clean-nljf
	make clean-jf
	rm -f package-lock.json



# Not used. Could be used for comparing inventory here
# vs that obtained by CDAS2HAPIinfo.js by walking HTML
# directory listing.
inventory:
	curl "$(CDAS)dataviews/sp_phys/datasets/AC_H2_MFI/inventory/19970829T000000Z,19970928T100010Z"
	# Returns
	# <Start>1997-09-02T00:00:00.000Z</Start><End>1997-09-28T23:00:00.000Z</End>

