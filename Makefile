# $ make
# $ make all
all: info

GIT = git
.PHONY: info $(GIT)

# $ make info
info:
	@echo "GIT: $(GIT)"

# $ make git
git:
	@bash ./scripts/git.sh
	