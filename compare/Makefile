
all:
	make AC_H2_CRIS
	echo -e "\n\n"
	make AMPTECCE_H0_MEPA
	echo -e "\n\n"
	make OMNI_HRO_1MIN

compare:
	@head -3 data/$(ID).hapi.csv | sed 's/e+00//' | sed 's/e/E/' > data/$(ID).hapi.csv.tmp 
	@head -3 data/$(ID).cdas.csv > data/$(ID).cdas.csv.tmp
	@echo -e "\n\n"
	@echo "---- Diff of HAPI Production CSV to CDAS CSV from text response ----"
	@diff -w data/$(ID).hapi.csv.tmp data/$(ID).cdas.csv.tmp && echo "No differences"
	@echo "--------------------------------------------------------------------"

AC_H2_CRIS:
	bash compare-data.sh \
		"AC_H2_CRIS" "flux_B" \
		"2010-06-21T14:50:52Z" "2010-07-30T14:50:52Z" \
		2>&1 | tee data/AC_H2_CRIS.log
	@make compare ID=AC_H2_CRIS 2>&1 | tee data/A2_K0_MPA.log

AMPTECCE_H0_MEPA:
	bash compare-data.sh \
		"AMPTECCE_H0_MEPA@0" "SpinPeriod" \
		"1985-06-21T14:50:52.472Z" "1985-06-26T00:00:00.477Z" \
		2>&1 | tee data/"AMPTECCE_H0_MEPA@0".log
	@make compare ID=AMPTECCE_H0_MEPA 2>&1 | tee data/AMPTECCE_H0_MEPA.log

OMNI_HRO_1MIN:
	bash compare-data.sh \
		"OMNI_HRO_1MIN" "SYM_H" \
		"2010-06-01T00:00:00Z" "2010-07-01T00:00:00Z" \
		2>&1 | tee data/OMNI_HRO_1MIN.log
	@make compare ID=OMNI_HRO_1MIN 2>&1 | tee data/OMNI_HRO_1MIN.log

