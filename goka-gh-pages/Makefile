

copy:
	rm -r public
	hugo
	cd ../goka && git checkout _docs_ && rm -r docs
	cp -r public ../docs
	# commit manually  for now :) 