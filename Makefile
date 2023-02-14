IDREGEX=^AC_

all: node_modules
	make cdas
	make hapinl
	make hapibh

# Install nodejs dependencies
node_modules:
	npm install

cdas:
	node CDAS2HAPI.js --idregex '$(IDREGEX)'

# Nand's Lal's (nl) production HAPI server
hapinl:
	node HAPI.js --version 'nl' --idregex '$(IDREGEX)'	

# Bernie Harris' (bh) prototype HAPI server
hapibh:
	node HAPI.js --version 'bh' --idregex '$(IDREGEX)'	

distclean:
	@rm -rf node_modules/

clean-cdas:
	@rm -f all-cdas.json
	@rm -f all-cdas-full.json
	@rm -rf cdas/

clean-hapinl:
	@rm -f all-hapinl.json
	@rm -rf hapinl/

clean-hapibh:
	@rm -f all-hapibh.json
	@rm -rf hapibh/

clean:
	make clean-cdas
	make clean-hapibh
	make clean-hapinl
	@rm -f package-lock.json
