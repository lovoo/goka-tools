.PHONY: preview build clean mod-update mod-tidy mod-clean help

help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

preview: ## Start Hugo development server
	hugo server --buildDrafts

build: ## Build the Hugo site for production
	hugo

clean: ## Remove the public directory
	rm -rf public resources

mod-update: ## Update Hugo theme modules to latest version
	hugo mod get -u
	hugo mod tidy

mod-tidy: ## Download and tidy Hugo module dependencies
	hugo mod tidy

mod-clean: ## Clean Hugo module cache
	hugo mod clean
