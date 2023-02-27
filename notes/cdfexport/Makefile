SHELL=/bin/zsh
BASE=ac_or_def_19970826_v01
CLASSPATH=.:$(CDF_BASE)/cdfjava/classes/cdfjava.jar:/$(CDF_BASE)/cdfjava/classes/cdfjson.jar:$(CDF_BASE)/cdfjava/classes/gson-2.8.6.jar:$(CDF_BASE)/cdfjava/classes/javax.json-1.0.4.jar:$(CDF_BASE)/cdfjava/cdftools/CDFToolsDriver.jar:$(CDF_BASE)/cdfjava/cdfml/cdfml.jar
#echo $(shell env)

all:
	make $(BASE).cdf
	rm -f $(BASE).reduced.cdf
	DYLD_LIBRARY_PATH=.:$(CDF_BASE)/lib $(CDF_BASE)/bin/cdfexport \
			-batch cdf -cdf ac_or_def_19970826_v01.reduced.cdf \
			-recordrange "1,2" \
			ac_or_def_19970826_v01
	CLASSPATH=$(CLASSPATH) java CDF2CDFML -withZ $(BASE).reduced.cdf
	grep "<record" $(BASE).reduced.xml

$(BASE).cdf:
	curl -s -O https://cdaweb.gsfc.nasa.gov/sp_phys/data/ace/orbit/level_2_cdaweb/def_or/1997/$(BASE).cdf

clean:
	rm -f $(BASE).*