all:
	make AC_H2_CRIS
	echo -e "\n\n"
	make C1_JP_PSE
	echo -e "\n\n"
	make THA_L2_MOM
	echo -e "\n\n"
	make "AMPTECCE_H0_MEPA@0"
	echo -e "\n\n"
	make OMNI_HRO_1MIN

single:
	bash compare-data.sh \
		"$(ID)" "$(PARAMS)" "$(START)" "$(STOP)" \
		2>&1 | tee data/$(ID).log
	#@make compare ID="$(ID)" 2>&1 | tee data/"$(ID)".log

compare:
	@head -3 data/$(ID).hapi.csv | sed 's/e+00//' | sed 's/e/E/' > data/$(ID).hapi.csv.tmp 
	@head -3 data/$(ID).cdas.csv > data/$(ID).cdas.csv.tmp
	@echo -e "\n\n"
	@echo "---- Diff of HAPI Production CSV to CDAS CSV from text response ----"
	@diff -w data/$(ID).hapi.csv.tmp data/$(ID).cdas.csv.tmp && echo "No differences"
	@echo "--------------------------------------------------------------------"

AC_H2_CRIS:
	make single ID=AC_H2_CRIS PARAMS=flux_B START="2010-06-21T14:50:52Z" STOP="2010-07-30T14:50:52Z"

AMPTECCE_H0_MEPA@0:
	make single ID="AMPTECCE_H0_MEPA@0" PARAMS=SpinPeriod START="1985-06-21T14:50:52.472Z" STOP="1985-06-26T00:00:00.477Z"

C1_JP_PSE:
	make single ID=C1_JP_PSE PARAMS=event_code__C1_JP_PSE START="2000-08-01T00:00:00Z" STOP="2000-08-20T00:00:00.000Z"

OMNI_HRO_1MIN:
	make single ID="OMNI_HRO_1MIN" PARAMS=SYM_H START="2010-06-01T00:00:00Z" STOP="2010-07-01T00:00:00Z"

THA_L2_MOM:
	make single ID=THA_L2_MOM PARAMS=tha_peim_densityQ START="2007-08-02T00:00:30.705Z" STOP="2007-08-02T00:00:36.723Z"


