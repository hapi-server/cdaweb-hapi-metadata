compare-meta:
	node compare-meta.js

compare-data1:
	mkdir -p data && node compare-data.js \
		--id AC_H2_MFI \
		--parameters Magnitude \
		--start '2008-06-01T00:00:00.000000000Z' \
		--stop '2008-06-01T01:00:00.000000000Z' \
		--debug

# All records for one year
compare-data2:
	mkdir -p data && node compare-data.js \
		--id AC_H2_MFI \
		--parameters Magnitude \
		--start '2008-01-01T00:00:00.000000000Z' \
		--stop '2009-01-01T00:00:00.000000000Z' \

# All records for two years
compare-data3:
	mkdir -p data && node compare-data.js \
		--id AC_H2_MFI \
		--parameters Magnitude \
		--start '2006-01-01T00:00:00.000000000Z' \
		--stop '2008-01-01T00:00:00.000000000Z' \

#   --summary \
#   --debug

# 1 record returned for all variables.
compare-data4:
	mkdir -p data && node compare-data.js \
		--id AC_H3_CRIS \
		--start '2008-06-01T00:00:00.000000000Z' \
		--stop '2008-06-01T01:00:00.000000000Z' \
		--showdata