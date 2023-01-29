
GOKA_PATH?=${GOPATH}/src/github.com/lovoo/goka


# setup the document structure (only first time)
setup:
	git submodule update --init --recursive
	npm install -D autoprefixer postcss-cli postcss

serve-local:
	hugo serve

copy:
	-rm -r public
	hugo
	cd ${GOKA_PATH} && git checkout _docs_ && touch docs && rm -r docs
	cp -r public ${GOKA_PATH}/docs
	# commit manually  for now :) 