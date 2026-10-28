# Conventions for editing this repo

This repo is a design held in three documents and in GitHub issues; code starts at stage 1 ([README](README.md#14-development-stages)). Read [README.md](README.md), [DESIGN-LOG.md](DESIGN-LOG.md) and [ROADMAP.md](ROADMAP.md) before changing any of them.

## Where things go

- **README.md is the spec.** Change it first.
- **DESIGN-LOG.md** gets an entry whenever a change corrects, reverses or rescopes an earlier decision: what it was, what it is now, and how the flaw was found.
- **ROADMAP.md** holds the phases after stage 7 and a parking lot of small items.
- **Issues** track the work: one milestone per development stage (0 to 7; stage 8 moved to phase G) and one per roadmap phase ("Later A" to "Later H"). After a design change, update every issue it affects.

## Git

- **Pull requests are merged by rebase only.** GitHub's rebase merge gives every commit a new hash, even when the branch is already up to date.
- **Never write a commit hash into the docs,** and don't add commits that only insert links. The design log has no link column; its header says how to find the change behind an entry.
- **Link pull requests and issues by full URL** in the docs; a bare `#number` is not a link in a Markdown file.

## Writing

- **Never renumber README sections or the numbered items in README section 12.** Issues cite them by number. Add a subsection or a new item at the end instead.
- **Mark every claim:** *verified* (checked against the cited source), *estimate* (reasoning, to measure), *recollection, verify* (believed, not checked). Check TigerBeetle and Kafka claims against their source code and cite the function.
- **Plain, short sentences.** No code in the docs beyond identifiers.

## Issues

- **A body states the goal, what "done" means, and the README section it implements.** Don't copy README detail into issues; it drifts. Don't cite follow-up numbers that may move.
- **End with the line the other issues use,** for example `Stage: 2. Pure core — see [Development stages](https://github.com/nmarasoiu/dst-200ktps-ledger#14-development-stages).` or `Phase: Later A: Card lifecycle — see [ROADMAP.md](https://github.com/nmarasoiu/dst-200ktps-ledger/blob/main/ROADMAP.md).`

## Local git clones if u are a local agent
../../distri/db/tigerbeetle
