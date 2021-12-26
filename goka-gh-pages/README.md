# Goka documentation on github pages

## Developing
```bash
cd goka-gh-pages
# only first time
git submodule update --init --recursive
# serve content
hugo serve
# happy documenting!
```


## Deploying
The generated content is hosted in branch `_docs_` in the folder `docs` in the goka main repo.

Deployment currently is done manually by coping over the hugo-output and committing the changes

`make copy`