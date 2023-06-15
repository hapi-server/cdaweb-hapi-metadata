IDREGEX=^AC_OR
CDAS=https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/
#AUTOPLOT=http://autoplot.org/devel/autoplot.jar
AUTOPLOT=https://ci-pw.physics.uiowa.edu/job/autoplot-release-2022/lastSuccessfulBuild/artifact/autoplot/Autoplot/dist/autoplot.jar

.PHONY: all bw jf nl bh

# Time varying bins
# RBSPB_REL04_ECT-HOPE-SCI-L2SA

#all: node_modules bin/autoplot.jar
all: node_modules
	@mkdir -p hapi
	@echo "\n-----bw------\n"
	make bw IDREGEX=$(IDREGEX)
	@echo "\n-----nl------\n"
	make nl IDREGEX=$(IDREGEX)
	@echo "\n-----bh------\n"
	make bh IDREGEX=$(IDREGEX)
	#@echo "\n-----jf------\n"
	#make jf IDREGEX=$(IDREGEX)

bw: node_modules
	node CDAS2HAPIinfo.js --keepids '$(IDREGEX)'

# Nand's Lal's (nl) production HAPI server
nl:
	node bin/HAPIinfo.js --version 'nl' --keepids '$(IDREGEX)'

# Jeremy Faden's (jf) test version of nl's server
nljf:
	node bin/HAPIinfo.js --version 'nljf' --keepids '$(IDREGEX)' --hapiurl 'https://jfaden.net/server/cdaweb/hapi'

# Bernie Harris' (bh) prototype HAPI server
bh:
	node bin/HAPIinfo.js --version 'bh' --keepids '$(IDREGEX)'	

# Jeremy Faden's (jf) AutoplotDataServer HAPI server
jf: bin/autoplot.jar
	node bin/HAPIinfo.js --version 'jf' --keepids '$(IDREGEX)'	

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

rsync-pull:
	rsync -avz --delete weigel@mag.gmu.edu:www/git-data/cdaweb-hapi-metadata/hapi
	rsync -avz --delete weigel@mag.gmu.edu:www/git-data/cdaweb-hapi-metadata/cache
	rsync -avz --delete verify/ weigel@mag.gmu.edu:www/git-data/cdaweb-hapi-metadata/verify/data

rsync-push:
	rsync -avz --delete hapi weigel@mag.gmu.edu:www/git-data/cdaweb-hapi-metadata
	rsync -avz --delete verify/data weigel@mag.gmu.edu:www/git-data/cdaweb-hapi-metadata/verify
	rsync -avz --delete cache weigel@mag.gmu.edu:www/git-data/cdaweb-hapi-metadata
