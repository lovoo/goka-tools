# Goka documentation on github pages

## Start locally
```bash
cd goka-gh-pages
make setup # only before the first start
make serve-local
```


## Deploying
The generated content is hosted in branch `_docs_` in the folder `docs` in the goka main repo.

Deployment currently is done manually by coping over the hugo-output and committing the changes

`make copy`